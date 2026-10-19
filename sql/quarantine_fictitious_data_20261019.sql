-- Script de Quarentena Transacional e Reversível de Dados Fictícios
-- Data de Execução: 2026-10-19
-- Contexto: Preflight Pregão Piloto WINZ26
-- Origem dos dados fictícios: Testes de integração prévios ingeridos em 2026-10-18 com timestamps futuros de 2026-10-19

BEGIN;

-- 1. Criação da tabela de quarentena para ticks brutos
CREATE TABLE IF NOT EXISTS quarantine_market_ticks_raw (
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
    quarantined_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    quarantine_reason TEXT NOT NULL,
    PRIMARY KEY (time_utc, symbol, tick_fingerprint)
);

-- 2. Criação da tabela de quarentena para candles M2
CREATE TABLE IF NOT EXISTS quarantine_candles_m2 (
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
    quarantined_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    quarantine_reason TEXT NOT NULL,
    PRIMARY KEY (symbol, open_time_utc)
);

-- 3. Inserção dos 46 ticks fictícios na quarentena
INSERT INTO quarantine_market_ticks_raw (
    symbol, time_msc, tick_fingerprint, time_utc, price,
    volume_real, flags, trade_side, occurrence_ordinal,
    provenance, ingested_at_utc, quarantined_at_utc, quarantine_reason
)
SELECT 
    symbol, time_msc, tick_fingerprint, time_utc, price,
    volume_real, flags, trade_side, occurrence_ordinal,
    provenance, ingested_at_utc, NOW(),
    'Dados de teste inseridos em 2026-10-18 antes do pregão piloto de 2026-10-19'
FROM market_ticks_raw_2026_10_19
WHERE ingested_at_utc < '2026-10-19 00:00:00+00'
  AND (tick_fingerprint LIKE 'fp_real_pg_%' 
    OR tick_fingerprint LIKE 'fp_dedup_real_%' 
    OR tick_fingerprint LIKE 'fp_audit_recon_%');

-- 4. Remoção restrita aos registros movidos para quarentena
DELETE FROM market_ticks_raw_2026_10_19
WHERE (time_utc, symbol, tick_fingerprint) IN (
    SELECT time_utc, symbol, tick_fingerprint FROM quarantine_market_ticks_raw
);

-- 5. Inserção do candle M2 fictício na quarentena
INSERT INTO quarantine_candles_m2 (
    symbol, open_time_utc, close_time_utc, open, high, low, close,
    volume_real, tick_volume, spread, is_closed, provenance,
    received_at_utc, ingested_at_utc, quarantined_at_utc, quarantine_reason
)
SELECT 
    symbol, open_time_utc, close_time_utc, open, high, low, close,
    volume_real, tick_volume, spread, is_closed, provenance,
    received_at_utc, ingested_at_utc, NOW(),
    'Candle M2 de teste inserido em 2026-10-18 antes do pregão piloto de 2026-10-19'
FROM candles_m2
WHERE ingested_at_utc < '2026-10-19 00:00:00+00'
  AND symbol = 'WINZ26'
  AND open_time_utc = '2026-10-19 12:00:00+00';

-- 6. Remoção do candle M2 movido
DELETE FROM candles_m2
WHERE (symbol, open_time_utc) IN (
    SELECT symbol, open_time_utc FROM quarantine_candles_m2
);

-- 7. Registro de auditoria no log de qualidade de dados
INSERT INTO data_quality_log (
    event_time_utc,
    event_type,
    symbol,
    ticks_recovered,
    candles_recovered,
    broker_status,
    provenance_uncertainty,
    description
) VALUES (
    NOW(),
    'QUARANTINE_FICTITIOUS_DATA',
    'WINZ26',
    46,
    1,
    'PREFLIGHT_OK',
    FALSE,
    'Quarentena transacional de 46 ticks e 1 candle M2 fictícios prévios ao pregão de 19/10/2026'
);

COMMIT;
