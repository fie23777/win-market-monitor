"""
Módulo de Telemetria Avançada Ponta a Ponta e Correlação Volume-Latência.
Atende rigorosamente às diretrizes do usuário:
1. Latência ponta a ponta (e2e) calculada individualmente por negócio (não por soma de percentis).
2. Decomposição individual: captura e replicação por negócio.
3. Correlação estatística entre picos de latência e taxa de negócios por segundo (TPS).
4. Distinção clara: Total sob Watermark Reconciliado vs Ticks Recentes Pendentes no SQLite / em trânsito.
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

def run_e2e_telemetry(sample_limit=5000):
    print("Coletando telemetria ponta a ponta e análise de correlação...")
    
    # 1. Obter Watermarks e Checkpoints do SQLite
    conn_sq = sqlite3.connect(config.buffer.sqlite_path)
    conn_sq.row_factory = sqlite3.Row
    cur_sq = conn_sq.cursor()
    cur_sq.execute("SELECT last_captured_msc, last_replicated_msc, updated_at_utc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
    ckp = cur_sq.fetchone()
    watermark_msc = ckp["last_replicated_msc"]
    captured_msc = ckp["last_captured_msc"]

    # Contagem sob Watermark
    cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE time_msc <= ?;", (watermark_msc,))
    sq_under_watermark = cur_sq.fetchone()[0]

    # Contagem Total no Spool (inclui ticks além do watermark)
    cur_sq.execute("SELECT count(*) FROM spool_ticks;")
    sq_total_all = cur_sq.fetchone()[0]

    # Status dos ticks
    cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE status = 'PENDING';")
    sq_pending = cur_sq.fetchone()[0]
    
    cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE status = 'COMMITTED';")
    sq_committed = cur_sq.fetchone()[0]

    # Amostra recente do spool com created_at_utc para decomposição de captura
    cur_sq.execute("""
    SELECT tick_fingerprint, time_msc, created_at_utc 
    FROM spool_ticks 
    WHERE provenance = 'REALTIME_STREAM'
    ORDER BY time_msc DESC 
    LIMIT ?;
    """, (sample_limit,))
    spool_samples = {r["tick_fingerprint"]: r for r in cur_sq.fetchall()}
    conn_sq.close()

    # 2. PostgreSQL sob Watermark e Amostra Ponta a Ponta
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
    pg_under_watermark = pg_row[0]
    pg_hist = pg_row[1]
    pg_rt = pg_row[2]

    # Amostra recente para cálculo individual ponta a ponta
    cur_pg.execute("""
    SELECT 
        tick_fingerprint,
        time_utc,
        ingested_at_utc,
        time_msc,
        EXTRACT(EPOCH FROM (ingested_at_utc - time_utc)) * 1000.0 as e2e_latency_ms,
        date_trunc('second', time_utc) as sec_bucket
    FROM market_ticks_raw
    WHERE provenance = 'REALTIME_STREAM' AND time_utc >= '2026-10-19 00:00:00+00'
    ORDER BY time_msc DESC
    LIMIT %s;
    """, (sample_limit,))
    pg_samples = cur_pg.fetchall()

    # Candles M2
    cur_pg.execute("""
    SELECT open_time_utc, close_time_utc, open, high, low, close, tick_volume, volume_real
    FROM candles_m2
    WHERE open_time_utc >= '2026-10-19 00:00:00+00'
    ORDER BY open_time_utc ASC;
    """)
    candles_rows = cur_pg.fetchall()

    # Taxa de negócios por segundo (TPS) nos últimos 15 minutos no PostgreSQL
    cur_pg.execute("""
    SELECT 
        date_trunc('second', time_utc) as sec,
        count(*) as tps,
        avg(EXTRACT(EPOCH FROM (ingested_at_utc - time_utc)) * 1000.0) as avg_e2e_ms,
        max(EXTRACT(EPOCH FROM (ingested_at_utc - time_utc)) * 1000.0) as max_e2e_ms
    FROM market_ticks_raw
    WHERE provenance = 'REALTIME_STREAM' AND time_utc >= NOW() - INTERVAL '15 minutes'
    GROUP BY date_trunc('second', time_utc)
    ORDER BY sec ASC;
    """)
    tps_rows = cur_pg.fetchall()
    conn_pg.close()

    # 3. Cálculo Rigoroso de Latência Individual por Negócio
    e2e_latencies = []
    capture_latencies = []
    replication_latencies = []

    for r in pg_samples:
        fp = r[0]
        time_utc = r[1]
        ingested_utc = r[2]
        time_msc = r[3]
        e2e_ms = float(r[4])

        e2e_latencies.append(e2e_ms)

        # Decomposição individual se presente no spool
        if fp in spool_samples:
            sp_row = spool_samples[fp]
            try:
                spool_created_utc = datetime.fromisoformat(sp_row["created_at_utc"])
                event_utc = normalizer.server_msc_to_utc_datetime(sp_row["time_msc"])
                
                cap_ms = (spool_created_utc - event_utc).total_seconds() * 1000.0
                rep_ms = (ingested_utc - spool_created_utc).total_seconds() * 1000.0
                
                if cap_ms >= 0:
                    capture_latencies.append(cap_ms)
                if rep_ms >= 0:
                    replication_latencies.append(rep_ms)
            except Exception:
                pass

    def calc_stats(arr):
        if not arr:
            return {"mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "count": 0}
        return {
            "mean_ms": round(float(np.mean(arr)), 2),
            "p50_ms": round(float(np.percentile(arr, 50)), 2),
            "p95_ms": round(float(np.percentile(arr, 95)), 2),
            "p99_ms": round(float(np.percentile(arr, 99)), 2),
            "max_ms": round(float(np.max(arr)), 2),
            "count": len(arr)
        }

    e2e_stats = calc_stats(e2e_latencies)
    cap_stats = calc_stats(capture_latencies)
    rep_stats = calc_stats(replication_latencies)

    # 4. Análise de Correlação Volume vs Latência (TPS vs Latência)
    correlation_data = []
    tps_values = []
    avg_lat_values = []
    for r in tps_rows:
        sec_dt = r[0]
        tps = int(r[1])
        avg_lat = float(r[2]) if r[2] is not None else 0.0
        max_lat = float(r[3]) if r[3] is not None else 0.0
        tps_values.append(tps)
        avg_lat_values.append(avg_lat)
        if tps > 50 or max_lat > 1000.0:  # registrar momentos de alta demanda ou latência elevada
            correlation_data.append({
                "time_brt": sec_dt.astimezone(BRT_TZ).strftime("%H:%M:%S"),
                "tps": tps,
                "avg_e2e_ms": round(avg_lat, 2),
                "max_e2e_ms": round(max_lat, 2)
            })

    # Coeficiente de correlação de Pearson entre TPS e latência média
    corr_coef = 0.0
    if len(tps_values) > 5 and len(avg_lat_values) > 5:
        c_matrix = np.corrcoef(tps_values, avg_lat_values)
        corr_coef = round(float(c_matrix[0, 1]), 4) if not np.isnan(c_matrix[0, 1]) else 0.0

    # 5. Continuidade dos Candles M2
    candle_gaps = []
    for i in range(1, len(candles_rows)):
        prev_close = candles_rows[i-1][1]
        curr_open = candles_rows[i][0]
        if prev_close != curr_open:
            candle_gaps.append((str(prev_close), str(curr_open)))

    # 6. Estado de Recursos e Processo
    pid = 3016
    pid_active = psutil.pid_exists(pid)
    proc = psutil.Process(pid) if pid_active else None
    mem_mb = proc.memory_info().rss / (1024 * 1024) if proc else 0.0
    cpu_pct = proc.cpu_percent(interval=0.1) if proc else 0.0

    now_brt = datetime.now(timezone.utc).astimezone(BRT_TZ)
    watermark_dt_brt = normalizer.server_msc_to_market_datetime(watermark_msc)

    report = {
        "report_time_brt": now_brt.strftime("%Y-%m-%d %H:%M:%S"),
        "watermark_msc": watermark_msc,
        "watermark_brt": watermark_dt_brt.strftime("%H:%M:%S.%f")[:-3],
        "pid": pid,
        "pid_active": pid_active,
        "cpu_percent": round(cpu_pct, 1),
        "ram_rss_mb": round(mem_mb, 2),
        "counts": {
            "reconciled_under_watermark": {
                "sqlite_ticks": sq_under_watermark,
                "postgres_ticks": pg_under_watermark,
                "discrepancy": sq_under_watermark - pg_under_watermark,
                "postgres_historical": pg_hist,
                "postgres_realtime": pg_rt,
                "sum_check_match": (pg_hist + pg_rt) == pg_under_watermark
            },
            "recent_unreconciled_activity": {
                "sqlite_total_buffered": sq_total_all,
                "sqlite_pending_replication": sq_pending,
                "sqlite_committed_local": sq_committed,
                "ticks_buffered_beyond_watermark": sq_total_all - sq_under_watermark
            }
        },
        "latencies": {
            "end_to_end_individual": e2e_stats,
            "capture_individual": cap_stats,
            "replication_individual": rep_stats
        },
        "volume_latency_correlation": {
            "pearson_correlation_tps_vs_latency": corr_coef,
            "total_seconds_analyzed": len(tps_values),
            "max_tps_observed": max(tps_values) if tps_values else 0,
            "high_load_events_sample": correlation_data[:10]
        },
        "candles_m2": {
            "total_closed": len(candles_rows),
            "distinct_open": len(set(r[0] for r in candles_rows)),
            "gaps_count": len(candle_gaps),
            "first_candle_brt": candles_rows[0][0].astimezone(BRT_TZ).strftime("%H:%M:%S") if candles_rows else None,
            "last_candle_brt": candles_rows[-1][1].astimezone(BRT_TZ).strftime("%H:%M:%S") if candles_rows else None,
            "minutes_covered": len(candles_rows) * 2
        }
    }

    out_file = Path("scratch/e2e_telemetry_snapshot.json")
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return report

if __name__ == "__main__":
    run_e2e_telemetry()
