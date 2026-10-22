"""
Módulo de Telemetria Avançada e Estatística de Latência.
Mede métricas de percentis (média, p95, p99, max), separando atraso de captura e replicação.
"""

import sys
sys.path.insert(0, ".")
import sqlite3
import psycopg2
import hashlib
import json
import psutil
import numpy as np
import MetaTrader5 as mt5
from pathlib import Path
from datetime import datetime, timezone
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

config = load_config()
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

def collect_telemetry_snapshot():
    # 1. Obter Watermark Congelado
    conn_sq = sqlite3.connect(config.buffer.sqlite_path)
    conn_sq.row_factory = sqlite3.Row
    cur_sq = conn_sq.cursor()
    cur_sq.execute("SELECT last_captured_msc, last_replicated_msc, updated_at_utc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
    ckp = cur_sq.fetchone()
    watermark_msc = ckp["last_replicated_msc"]
    captured_msc = ckp["last_captured_msc"]

    # 2. Reconciliação sob Watermark
    cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE time_msc <= ?;", (watermark_msc,))
    sq_total = cur_sq.fetchone()[0]

    cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE status = 'PENDING';")
    sq_pending_total = cur_sq.fetchone()[0]

    # Amostra recente de latências no spool (diferença entre ingested e time_msc)
    cur_sq.execute("""
    SELECT time_msc, created_at_utc 
    FROM spool_ticks 
    WHERE provenance = 'REALTIME_STREAM'
    ORDER BY time_msc DESC 
    LIMIT 2000;
    """)
    recent_spool_rows = cur_sq.fetchall()
    conn_sq.close()

    # 3. PostgreSQL sob Watermark
    conn_pg = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        dbname=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password
    )
    conn_pg.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ, readonly=True)
    cur_pg = conn_pg.cursor()

    cur_pg.execute("""
    SELECT 
        count(*) as total,
        count(*) FILTER (WHERE provenance = 'RECOVERED_HISTORICAL_MT5') as historical,
        count(*) FILTER (WHERE provenance = 'REALTIME_STREAM') as realtime
    FROM market_ticks_raw
    WHERE time_utc >= '2026-10-19 00:00:00+00' AND time_msc <= %s;
    """, (watermark_msc,))
    pg_row = cur_pg.fetchone()
    pg_total = pg_row[0]
    pg_hist = pg_row[1]
    pg_rt = pg_row[2]

    # Amostra recente de replicação (diferença entre ingested_at_utc no PG e time_utc)
    cur_pg.execute("""
    SELECT EXTRACT(EPOCH FROM (ingested_at_utc - time_utc)) * 1000.0 as rep_latency_ms
    FROM market_ticks_raw
    WHERE provenance = 'REALTIME_STREAM' AND time_utc >= '2026-10-19 00:00:00+00'
    ORDER BY time_msc DESC
    LIMIT 2000;
    """)
    rep_lat_rows = [float(r[0]) for r in cur_pg.fetchall() if r[0] is not None]

    # Candles M2
    cur_pg.execute("SELECT count(*), count(DISTINCT open_time_utc), min(open_time_utc), max(close_time_utc) FROM candles_m2 WHERE open_time_utc >= '2026-10-19 00:00:00+00';")
    c_tot, c_dist, c_min, c_max = cur_pg.fetchone()
    conn_pg.close()

    # 4. Cálculo de Latências e Percentis
    # Atraso de Captura (do evento na B3 até o commit no SQLite Spool)
    capture_delays = []
    for r in recent_spool_rows:
        try:
            created_utc = datetime.fromisoformat(r["created_at_utc"])
            event_utc = normalizer.server_msc_to_utc_datetime(r["time_msc"])
            delay_ms = (created_utc - event_utc).total_seconds() * 1000.0
            if delay_ms >= 0:
                capture_delays.append(delay_ms)
        except Exception:
            pass

    cap_stats = {
        "mean_ms": round(float(np.mean(capture_delays)), 2) if capture_delays else 0.0,
        "p50_ms": round(float(np.percentile(capture_delays, 50)), 2) if capture_delays else 0.0,
        "p95_ms": round(float(np.percentile(capture_delays, 95)), 2) if capture_delays else 0.0,
        "p99_ms": round(float(np.percentile(capture_delays, 99)), 2) if capture_delays else 0.0,
        "max_ms": round(float(np.max(capture_delays)), 2) if capture_delays else 0.0,
        "sample_size": len(capture_delays)
    }

    rep_stats = {
        "mean_ms": round(float(np.mean(rep_lat_rows)), 2) if rep_lat_rows else 0.0,
        "p50_ms": round(float(np.percentile(rep_lat_rows, 50)), 2) if rep_lat_rows else 0.0,
        "p95_ms": round(float(np.percentile(rep_lat_rows, 95)), 2) if rep_lat_rows else 0.0,
        "p99_ms": round(float(np.percentile(rep_lat_rows, 99)), 2) if rep_lat_rows else 0.0,
        "max_ms": round(float(np.max(rep_lat_rows)), 2) if rep_lat_rows else 0.0,
        "sample_size": len(rep_lat_rows)
    }

    # 5. Estado do Processo e Recursos
    pid = 3016
    pid_active = psutil.pid_exists(pid)
    proc = psutil.Process(pid) if pid_active else None
    mem_mb = proc.memory_info().rss / (1024 * 1024) if proc else 0.0
    cpu_pct = proc.cpu_percent(interval=0.1) if proc else 0.0

    # MT5 Book Live
    mt5.initialize()
    t_info = mt5.terminal_info()
    tick_live = mt5.symbol_info_tick("WINZ26")
    mt5.shutdown()

    live_lag_ms = max(0, tick_live.time_msc - watermark_msc) if tick_live else 0

    now_brt = datetime.now(timezone.utc).astimezone(BRT_TZ)

    snapshot = {
        "timestamp_brt": now_brt.strftime("%Y-%m-%d %H:%M:%S"),
        "watermark_msc": watermark_msc,
        "watermark_brt": normalizer.server_msc_to_market_datetime(watermark_msc).strftime("%H:%M:%S.%f")[:-3],
        "pid": pid,
        "pid_active": pid_active,
        "mt5_connected": t_info.connected if t_info else False,
        "cpu_percent": round(cpu_pct, 1),
        "ram_rss_mb": round(mem_mb, 2),
        "counts_at_watermark": {
            "sqlite_ticks": sq_total,
            "postgres_total": pg_total,
            "postgres_historical": pg_hist,
            "postgres_realtime": pg_rt,
            "discrepancy": sq_total - pg_total,
            "backlog_pending": sq_pending_total
        },
        "capture_lag_stats": cap_stats,
        "replication_lag_stats": rep_stats,
        "instantaneous_book_lag_ms": live_lag_ms,
        "candles_m2": {
            "total_closed": c_tot,
            "distinct": c_dist,
            "duplicates": c_tot - c_dist,
            "minutes_covered": c_tot * 2,
            "first_candle": c_min.astimezone(BRT_TZ).strftime("%H:%M:%S") if c_min else None,
            "last_candle": c_max.astimezone(BRT_TZ).strftime("%H:%M:%S") if c_max else None
        }
    }
    return snapshot

if __name__ == "__main__":
    res = collect_telemetry_snapshot()
    print(json.dumps(res, indent=2, ensure_ascii=False))
