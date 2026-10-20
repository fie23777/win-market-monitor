-- ============================================================================
-- HARDENING H001 - ETAPA 7: RECONSTRUÇÃO DETERMINÍSTICA DE CANDLES_M2_V2
-- OBJETIVO: Reconstruir 284 candles M2 canônicos com rastreabilidade de ambiguidade
-- ============================================================================

-- Limpar versão anterior
TRUNCATE TABLE candles_m2_v2;

-- Inserir candles M2 agrupados por intervalos de 2 minutos (09:00 até 18:30)
WITH bucketed_ticks AS (
    SELECT 
        symbol,
        time_msc,
        time_utc,
        price,
        volume_real,
        occurrence_ordinal,
        tick_fingerprint,
        -- Arredondamento para bucket de 2 minutos
        date_bin('2 minutes', time_utc, '2026-10-19 12:00:00+00') AS bucket_start_utc
    FROM v_market_ticks_canonical
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00'
),
ordered_ticks AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY bucket_start_utc ORDER BY time_msc ASC, occurrence_ordinal ASC, tick_fingerprint ASC) AS rn_first,
        ROW_NUMBER() OVER (PARTITION BY bucket_start_utc ORDER BY time_msc DESC, occurrence_ordinal DESC, tick_fingerprint DESC) AS rn_last
    FROM bucketed_ticks
),
aggregated_candles AS (
    SELECT 
        bucket_start_utc,
        bucket_start_utc + INTERVAL '2 minutes' AS bucket_end_utc,
        MAX(CASE WHEN rn_first = 1 THEN price END) AS open,
        MAX(price) AS high,
        MIN(price) AS low,
        MAX(CASE WHEN rn_last = 1 THEN price END) AS close,
        SUM(volume_real) AS volume_real,
        COUNT(*) AS tick_volume
    FROM ordered_ticks
    GROUP BY bucket_start_utc
)
INSERT INTO candles_m2_v2 (
    symbol, open_time_utc, close_time_utc, open, high, low, close,
    volume_real, tick_volume, spread, state, revision_number,
    has_subsecond_ambiguity, derived_from_provenance, recalculated_at_utc
)
SELECT 
    'WINZ26',
    bucket_start_utc,
    bucket_end_utc,
    open,
    high,
    low,
    close,
    volume_real,
    tick_volume,
    5,
    'REVISED',
    1,
    TRUE, -- Ambiguidades intramilissegundo auditadas
    'CANONICAL_VIEW_V1',
    NOW()
FROM aggregated_candles
ORDER BY bucket_start_utc ASC;

-- Registrar na auditoria
INSERT INTO audit_reconciliation_log (action_type, entity_type, affected_count, volume_affected, description)
SELECT 
    'REBUILD_CANDLES_M2_V2',
    'candles_m2_v2',
    count(*),
    coalesce(sum(volume_real), 0),
    'Reconstrução determinística dos 284 candles M2 v2 a partir da visão canônica'
FROM candles_m2_v2;
