-- ============================================================================
-- HARDENING H001 - ETAPA 3: RECLASSIFICAÇÃO DE PROVENIÊNCIA (NC-001)
-- OBJETIVO: Reclassificar os 44.899 negócios do blackout (14:07 - 14:23 BRT)
-- ============================================================================

CREATE TABLE IF NOT EXISTS h001_reclassified_nc001_ticks (
    tick_fingerprint VARCHAR(64) PRIMARY KEY,
    previous_provenance VARCHAR(32) NOT NULL,
    reclassified_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

WITH updated AS (
    UPDATE market_ticks_raw
    SET provenance = 'RECOVERED_GAP_MT5'
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 17:07:27+00'
      AND time_utc <= '2026-10-19 17:23:50+00'
      AND ingested_at_utc >= '2026-10-19 17:23:50+00'
      AND ingested_at_utc <= '2026-10-19 17:25:00+00'
      AND provenance = 'REALTIME_STREAM'
    RETURNING tick_fingerprint, volume_real
)
INSERT INTO h001_reclassified_nc001_ticks (tick_fingerprint, previous_provenance)
SELECT tick_fingerprint, 'REALTIME_STREAM' FROM updated
ON CONFLICT (tick_fingerprint) DO NOTHING;

INSERT INTO audit_reconciliation_log (action_type, entity_type, affected_count, volume_affected, description)
SELECT 
    'RECLASSIFY_PROVENANCE_NC001',
    'market_ticks_raw',
    count(*),
    0,
    'Reclassificação de proveniência para RECOVERED_GAP_MT5 dos ticks ingeridos pós-blackout de 19/10'
FROM h001_reclassified_nc001_ticks;
