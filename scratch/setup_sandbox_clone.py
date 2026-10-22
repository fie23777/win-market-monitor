import sys
sys.path.insert(0, ".")
import psycopg2
from pathlib import Path

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"
backup_dir = Path("F:/win-market-monitor-backup/session_closing_snapshot_20261019_214726")
csv_ticks = backup_dir / "market_ticks_raw_snapshot.csv"
csv_candles = backup_dir / "candles_m2_snapshot.csv"

print("=== 1. APLICANDO SCHEMA NO SANDBOX CLONE (POSTGRESQL 17 - PORTA 5435) ===")

conn = psycopg2.connect(SANDBOX_PG_URI)
cur = conn.cursor()

# 1. DDL das tabelas
ddl_script = """
CREATE TABLE IF NOT EXISTS collector_checkpoints (
    stream_key VARCHAR(32) PRIMARY KEY,
    last_captured_msc BIGINT NOT NULL,
    last_replicated_msc BIGINT NOT NULL,
    last_candle_close_utc TIMESTAMPTZ,
    updated_at_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS market_ticks_raw (
    symbol VARCHAR(16) NOT NULL,
    time_msc BIGINT NOT NULL,
    tick_fingerprint VARCHAR(64) NOT NULL,
    time_utc TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    volume_real DOUBLE PRECISION NOT NULL,
    flags INTEGER NOT NULL,
    trade_side VARCHAR(8) NOT NULL,
    occurrence_ordinal INTEGER NOT NULL DEFAULT 1,
    provenance VARCHAR(32) NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (time_utc, symbol, tick_fingerprint)
) PARTITION BY RANGE (time_utc);

CREATE INDEX IF NOT EXISTS idx_ticks_msc ON market_ticks_raw (symbol, time_msc);

-- Partições necessárias
CREATE TABLE IF NOT EXISTS market_ticks_raw_2026_10_19 PARTITION OF market_ticks_raw
    FOR VALUES FROM ('2026-10-19 00:00:00+00') TO ('2026-10-20 00:00:00+00');

CREATE TABLE IF NOT EXISTS market_ticks_raw_2026_10_13 PARTITION OF market_ticks_raw
    FOR VALUES FROM ('2026-10-13 00:00:00+00') TO ('2026-10-14 00:00:00+00');

CREATE TABLE IF NOT EXISTS candles_m2 (
    symbol VARCHAR(16) NOT NULL,
    open_time_utc TIMESTAMPTZ NOT NULL,
    close_time_utc TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume_real DOUBLE PRECISION NOT NULL,
    tick_volume BIGINT NOT NULL,
    spread INTEGER NOT NULL,
    is_closed BOOLEAN NOT NULL DEFAULT TRUE,
    provenance VARCHAR(32) NOT NULL,
    received_at_utc TIMESTAMPTZ NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (symbol, open_time_utc)
);

CREATE INDEX IF NOT EXISTS idx_candles_close ON candles_m2 (symbol, close_time_utc DESC);
"""

cur.execute(ddl_script)
conn.commit()
print("Schema e partições criados com sucesso no Sandbox Clone.")

# 2. Restaurar candles_m2 do CSV
print("\n=== 2. RESTAURANDO CANDLES_M2 NO SANDBOX CLONE ===")
with open(csv_candles, "r", encoding="utf-8") as f:
    cur.copy_expert("COPY candles_m2 FROM STDOUT WITH CSV HEADER", f)
conn.commit()

cur.execute("SELECT COUNT(*) FROM candles_m2;")
candles_count = cur.fetchone()[0]
print(f"Candles M2 restaurados no Sandbox Clone: {candles_count} (Esperado: 284)")

# 3. Restaurar market_ticks_raw do CSV
print("\n=== 3. RESTAURANDO MARKET_TICKS_RAW NO SANDBOX CLONE ===")
with open(csv_ticks, "r", encoding="utf-8") as f:
    cur.copy_expert("COPY market_ticks_raw FROM STDOUT WITH CSV HEADER", f)
conn.commit()

cur.execute("SELECT COUNT(*) FROM market_ticks_raw;")
ticks_count = cur.fetchone()[0]
print(f"Ticks restaurados no Sandbox Clone: {ticks_count} (Esperado: 2093236)")

cur.close()
conn.close()
print("\n[OK] Setup e restauração do Sandbox Clone 100% concluídos!")
