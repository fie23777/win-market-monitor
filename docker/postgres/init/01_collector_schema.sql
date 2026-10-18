-- Schema de Produção: win-market-monitor (PostgreSQL 17)
-- Fuso Horário de Armazenamento: UTC Puro

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

CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id BIGSERIAL PRIMARY KEY,
    event_time_utc TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    gap_start_utc TIMESTAMPTZ,
    gap_end_utc TIMESTAMPTZ,
    ticks_recovered BIGINT DEFAULT 0,
    candles_recovered BIGINT DEFAULT 0,
    broker_status VARCHAR(64),
    provenance_uncertainty BOOLEAN DEFAULT FALSE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS partition_archive_registry (
    partition_name VARCHAR(64) PRIMARY KEY,
    trading_date DATE NOT NULL,
    row_count BIGINT NOT NULL,
    parquet_path TEXT NOT NULL,
    parquet_sha256 VARCHAR(64) NOT NULL,
    backup_path TEXT NOT NULL,
    is_validated BOOLEAN NOT NULL DEFAULT FALSE,
    archived_at_utc TIMESTAMPTZ NOT NULL,
    dropped_at_utc TIMESTAMPTZ
);
