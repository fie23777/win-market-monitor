-- Script de Reversão da Quarentena de Dados Fictícios
-- Utilizar somente se for necessário desfazer a movimentação para quarentena
BEGIN;

INSERT INTO market_ticks_raw_2026_10_19 (
    symbol, time_msc, tick_fingerprint, time_utc, price,
    volume_real, flags, trade_side, occurrence_ordinal,
    provenance, ingested_at_utc
)
SELECT 
    symbol, time_msc, tick_fingerprint, time_utc, price,
    volume_real, flags, trade_side, occurrence_ordinal,
    provenance, ingested_at_utc
FROM quarantine_market_ticks_raw;

DELETE FROM quarantine_market_ticks_raw;

INSERT INTO candles_m2 (
    symbol, open_time_utc, close_time_utc, open, high, low, close,
    volume_real, tick_volume, spread, is_closed, provenance,
    received_at_utc, ingested_at_utc
)
SELECT 
    symbol, open_time_utc, close_time_utc, open, high, low, close,
    volume_real, tick_volume, spread, is_closed, provenance,
    received_at_utc, ingested_at_utc
FROM quarantine_candles_m2;

DELETE FROM quarantine_candles_m2;

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
    'ROLLBACK_QUARANTINE_DATA',
    'WINZ26',
    46,
    1,
    'ROLLBACK_TRIGGERED',
    FALSE,
    'Reversão da quarentena de 46 ticks e 1 candle M2 fictícios'
);

COMMIT;
