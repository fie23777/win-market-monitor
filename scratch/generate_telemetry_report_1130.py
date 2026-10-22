"""
Gerador de Telemetria e Reconciliação Periódica - win-market-monitor
Horário de Referência: Marco de 11:30 BRT (Sessão Operacional Supervisionada)
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
from datetime import datetime, timezone, timedelta
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

config = load_config()
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

def generate_report():
    print("Iniciando auditoria de telemetria...")
    
    # 1. Obter Watermark Congelado
    conn_sq = sqlite3.connect(config.buffer.sqlite_path)
    conn_sq.row_factory = sqlite3.Row
    cur_sq = conn_sq.cursor()
    cur_sq.execute("SELECT last_captured_msc, last_replicated_msc, updated_at_utc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
    ckp = cur_sq.fetchone()
    watermark_msc = ckp["last_replicated_msc"]
    captured_msc = ckp["last_captured_msc"]

    # 2. SQLite sob Watermark
    cur_sq.execute("BEGIN TRANSACTION;")
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
    LIMIT 3000;
    """)
    recent_spool_rows = cur_sq.fetchall()

    # Hash SQLite até Watermark
    print("Computando SHA-256 no SQLite...")
    cur_sq.execute("""
    SELECT tick_fingerprint 
    FROM spool_ticks 
    WHERE time_msc <= ?
    ORDER BY time_msc ASC, occurrence_ordinal ASC, tick_fingerprint ASC;
    """, (watermark_msc,))
    hasher_sq = hashlib.sha256()
    for r in cur_sq:
        hasher_sq.update(r["tick_fingerprint"].encode('utf-8'))
    sq_hash = hasher_sq.hexdigest()
    conn_sq.close()

    # 3. PostgreSQL sob Watermark
    print("Consultando PostgreSQL...")
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

    # Amostra recente de replicação
    cur_pg.execute("""
    SELECT EXTRACT(EPOCH FROM (ingested_at_utc - time_utc)) * 1000.0 as rep_latency_ms
    FROM market_ticks_raw
    WHERE provenance = 'REALTIME_STREAM' AND time_utc >= '2026-10-19 00:00:00+00'
    ORDER BY time_msc DESC
    LIMIT 3000;
    """)
    rep_lat_rows = [float(r[0]) for r in cur_pg.fetchall() if r[0] is not None]

    # Hash PostgreSQL até Watermark
    print("Computando SHA-256 no PostgreSQL...")
    cur_pg.execute("""
    SELECT tick_fingerprint 
    FROM market_ticks_raw 
    WHERE time_utc >= '2026-10-19 00:00:00+00' AND time_msc <= %s
    ORDER BY time_msc ASC, occurrence_ordinal ASC, tick_fingerprint ASC;
    """, (watermark_msc,))
    hasher_pg = hashlib.sha256()
    for r in cur_pg:
        hasher_pg.update(r[0].encode('utf-8'))
    pg_hash = hasher_pg.hexdigest()

    # Candles M2 e Continuidade
    cur_pg.execute("""
    SELECT open_time_utc, close_time_utc, open, high, low, close, tick_volume, volume_real
    FROM candles_m2
    WHERE open_time_utc >= '2026-10-19 00:00:00+00'
    ORDER BY open_time_utc ASC;
    """)
    candles_rows = cur_pg.fetchall()
    conn_pg.close()

    # Verificação estrita de contiguidade dos candles M2
    candle_gaps = []
    for i in range(1, len(candles_rows)):
        prev_close = candles_rows[i-1][1]
        curr_open = candles_rows[i][0]
        if prev_close != curr_open:
            candle_gaps.append((str(prev_close), str(curr_open)))

    # 4. Estatísticas de Latência
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

    # 5. Recursos e Processo
    pid = 3016
    pid_active = psutil.pid_exists(pid)
    proc = psutil.Process(pid) if pid_active else None
    mem_mb = proc.memory_info().rss / (1024 * 1024) if proc else 0.0
    cpu_pct = proc.cpu_percent(interval=0.1) if proc else 0.0

    mt5.initialize()
    t_info = mt5.terminal_info()
    tick_live = mt5.symbol_info_tick("WINZ26")
    mt5.shutdown()

    live_lag_ms = max(0, tick_live.time_msc - watermark_msc) if tick_live else 0

    now_brt = datetime.now(timezone.utc).astimezone(BRT_TZ)
    watermark_dt_brt = normalizer.server_msc_to_market_datetime(watermark_msc)

    summary = {
        "report_time_brt": now_brt.strftime("%Y-%m-%d %H:%M:%S"),
        "watermark_msc": watermark_msc,
        "watermark_brt": watermark_dt_brt.strftime("%H:%M:%S.%f")[:-3],
        "pid": pid,
        "pid_active": pid_active,
        "mt5_connected": t_info.connected if t_info else False,
        "cpu_percent": round(cpu_pct, 1),
        "ram_rss_mb": round(mem_mb, 2),
        "counts": {
            "sqlite_ticks": sq_total,
            "postgres_total": pg_total,
            "postgres_historical": pg_hist,
            "postgres_realtime": pg_rt,
            "historical_plus_realtime": pg_hist + pg_rt,
            "discrepancy": sq_total - pg_total,
            "backlog_pending": sq_pending_total
        },
        "hashes": {
            "sqlite_sha256": sq_hash,
            "postgres_sha256": pg_hash,
            "match": sq_hash == pg_hash
        },
        "capture_latency": cap_stats,
        "replication_latency": rep_stats,
        "instantaneous_book_lag_ms": live_lag_ms,
        "candles_m2": {
            "total_closed": len(candles_rows),
            "distinct_open": len(set(r[0] for r in candles_rows)),
            "gaps_count": len(candle_gaps),
            "gaps_details": candle_gaps,
            "first_candle_brt": candles_rows[0][0].astimezone(BRT_TZ).strftime("%H:%M:%S") if candles_rows else None,
            "last_candle_brt": candles_rows[-1][1].astimezone(BRT_TZ).strftime("%H:%M:%S") if candles_rows else None,
            "minutes_covered": len(candles_rows) * 2
        }
    }

    out_file = Path("scratch/telemetry_1130_summary.json")
    out_file.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    generate_report()
