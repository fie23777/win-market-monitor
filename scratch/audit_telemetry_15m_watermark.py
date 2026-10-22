import sys
sys.path.insert(0, ".")
import sqlite3
import psycopg2
import hashlib
import json
import psutil
import MetaTrader5 as mt5
from pathlib import Path
from datetime import datetime, timezone
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

config = load_config()
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

print("=== COLETANDO TELEMETRIA FORMAL DE 15 MINUTOS COM WATERMARK CONGELADO ===")

# 1. Determinar o Watermark Estável a partir do Checkpoint Atual
conn_sq = sqlite3.connect(config.buffer.sqlite_path)
conn_sq.row_factory = sqlite3.Row
cur_sq = conn_sq.cursor()
cur_sq.execute("SELECT last_captured_msc, last_replicated_msc, updated_at_utc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
ckp_row = cur_sq.fetchone()
watermark_msc = ckp_row["last_replicated_msc"]
last_captured_msc = ckp_row["last_captured_msc"]

# 2. Consulta no SQLite Spool congelada em uma transação atômica até watermark_msc
cur_sq.execute("BEGIN TRANSACTION;")
cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE time_msc <= ?;", (watermark_msc,))
sq_total_watermark = cur_sq.fetchone()[0]

cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE time_msc <= ? AND status = 'COMMITTED';", (watermark_msc,))
sq_committed_watermark = cur_sq.fetchone()[0]

cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE time_msc <= ? AND status = 'PENDING';", (watermark_msc,))
sq_pending_watermark = cur_sq.fetchone()[0]

cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE status = 'PENDING';")
sq_pending_total = cur_sq.fetchone()[0]

cur_sq.execute("SELECT count(*) FROM spool_ticks;")
sq_raw_current_total = cur_sq.fetchone()[0]

# Hash Canônico no SQLite até o watermark
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

# 3. Consulta no PostgreSQL em uma ÚNICA transação REPEATABLE READ até watermark_msc
conn_pg = psycopg2.connect(
    host=config.postgres.host,
    port=config.postgres.port,
    dbname=config.postgres.database,
    user=config.postgres.user,
    password=config.postgres.password
)
conn_pg.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ, readonly=True)
cur_pg = conn_pg.cursor()

# Consulta atômica única com contagens agrupadas no mesmo snapshot
cur_pg.execute("""
SELECT 
    count(*) as total,
    count(*) FILTER (WHERE provenance = 'RECOVERED_HISTORICAL_MT5') as historical,
    count(*) FILTER (WHERE provenance = 'REALTIME_STREAM') as realtime
FROM market_ticks_raw
WHERE time_utc >= '2026-10-19 00:00:00+00' AND time_msc <= %s;
""", (watermark_msc,))
pg_row = cur_pg.fetchone()
pg_total_watermark = pg_row[0]
pg_hist_watermark = pg_row[1]
pg_rt_watermark = pg_row[2]

# Primeiro e último tick até o watermark no PostgreSQL
cur_pg.execute("""
SELECT time_msc, time_utc, price, volume_real, provenance, tick_fingerprint
FROM market_ticks_raw
WHERE time_utc >= '2026-10-19 00:00:00+00' AND time_msc <= %s
ORDER BY time_msc ASC, occurrence_ordinal ASC
LIMIT 1;
""", (watermark_msc,))
first_t = cur_pg.fetchone()

cur_pg.execute("""
SELECT time_msc, time_utc, price, volume_real, provenance, tick_fingerprint
FROM market_ticks_raw
WHERE time_utc >= '2026-10-19 00:00:00+00' AND time_msc <= %s
ORDER BY time_msc DESC, occurrence_ordinal DESC
LIMIT 1;
""", (watermark_msc,))
last_t = cur_pg.fetchone()

# Hash Canônico no PostgreSQL até o watermark
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

# Auditoria dos Candles M2
cur_pg.execute("""
SELECT count(*), count(DISTINCT open_time_utc)
FROM candles_m2
WHERE open_time_utc >= '2026-10-19 00:00:00+00';
""")
c_tot, c_dist = cur_pg.fetchone()

cur_pg.execute("""
SELECT min(open_time_utc), max(close_time_utc)
FROM candles_m2
WHERE open_time_utc >= '2026-10-19 00:00:00+00';
""")
c_min, c_max = cur_pg.fetchone()
conn_pg.close()

# 4. Medição de Lag de Mercado e MT5
mt5.initialize()
t_info = mt5.terminal_info()
tick_now = mt5.symbol_info_tick("WINZ26")
mt5.shutdown()

sys_now_utc = datetime.now(timezone.utc)
sys_now_brt = sys_now_utc.astimezone(BRT_TZ)

# Metodologia de medição de lag:
# O MT5 indexa o tempo de mercado no relógio do servidor (America/Sao_Paulo).
# O tick_now.time_msc representa o instante exato do último evento transmitido pelo broker.
# O lag em relação ao book ao vivo é a diferença entre tick_now.time_msc e watermark_msc.
market_live_msc = tick_now.time_msc if tick_now else watermark_msc
processing_lag_ms = max(0, market_live_msc - watermark_msc)

# Verificar PID 3016
pid = 3016
pid_active = psutil.pid_exists(pid)
mem_mb = psutil.Process(pid).memory_info().rss / (1024 * 1024) if pid_active else 0.0

result = {
    "telemetry_timestamp_brt": sys_now_brt.strftime("%Y-%m-%d %H:%M:%S"),
    "watermark_msc": watermark_msc,
    "watermark_brt": normalizer.server_msc_to_market_datetime(watermark_msc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    "watermark_utc": normalizer.server_msc_to_utc_datetime(watermark_msc).isoformat(),
    "pid": pid,
    "pid_active": pid_active,
    "mt5_connected": t_info.connected if t_info else False,
    "memory_rss_mb": round(mem_mb, 2),
    "reconciliation_at_watermark": {
        "sqlite_ticks": sq_total_watermark,
        "sqlite_committed": sq_committed_watermark,
        "sqlite_pending": sq_pending_watermark,
        "postgres_total": pg_total_watermark,
        "postgres_historical": pg_hist_watermark,
        "postgres_realtime": pg_rt_watermark,
        "sum_hist_plus_rt": pg_hist_watermark + pg_rt_watermark,
        "sum_consistency": (pg_hist_watermark + pg_rt_watermark) == pg_total_watermark,
        "discrepancy_spool_vs_pg": sq_total_watermark - pg_total_watermark,
        "sqlite_sha256": sq_hash,
        "postgres_sha256": pg_hash,
        "hash_match": sq_hash == pg_hash
    },
    "market_lag": {
        "market_live_last_msc": market_live_msc,
        "watermark_msc": watermark_msc,
        "processing_lag_ms": processing_lag_ms,
        "processing_lag_sec": round(processing_lag_ms / 1000.0, 3),
        "backlog_pending_total": sq_pending_total
    },
    "candles_m2": {
        "total": c_tot,
        "distinct": c_dist,
        "duplicates": c_tot - c_dist,
        "total_minutes_covered": c_tot * 2,
        "first_candle_open_brt": c_min.astimezone(BRT_TZ).strftime("%H:%M:%S") if c_min else None,
        "last_candle_close_brt": c_max.astimezone(BRT_TZ).strftime("%H:%M:%S") if c_max else None
    },
    "boundary_ticks": {
        "first_tick": {
            "time_msc": first_t[0],
            "time_brt": first_t[1].astimezone(BRT_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "time_utc": first_t[1].isoformat(),
            "price": first_t[2],
            "volume_real": first_t[3],
            "provenance": first_t[4],
            "fingerprint": first_t[5]
        },
        "last_tick": {
            "time_msc": last_t[0],
            "time_brt": last_t[1].astimezone(BRT_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "time_utc": last_t[1].isoformat(),
            "price": last_t[2],
            "volume_real": last_t[3],
            "provenance": last_t[4],
            "fingerprint": last_t[5]
        }
    }
}

out_file = Path("C:/Users/fie23/.gemini/antigravity-ide/brain/ef92ffd5-2a0e-47f2-97b3-b07361021f8d/telemetry_15m_snapshot.json")
with open(out_file, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(json.dumps(result, indent=2, ensure_ascii=False))
