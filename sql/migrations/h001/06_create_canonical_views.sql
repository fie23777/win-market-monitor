-- ============================================================================
-- HARDENING H001 - ETAPA 6: IMPLEMENTAÇÃO DA VISÃO CANÔNICA VERSIONADA
-- OBJETIVO: Isolar duplicatas de transporte sem alterar dados brutos
-- ============================================================================

CREATE OR REPLACE VIEW v_market_ticks_canonical AS
SELECT 
    symbol,
    time_msc,
    time_utc,
    price,
    volume_real,
    flags,
    (flags & (32 | 64 | 8)) AS identity_flags,
    trade_side,
    occurrence_ordinal,
    tick_fingerprint,
    provenance,
    ingested_at_utc
FROM market_ticks_raw
WHERE provenance <> 'DUPLICATE_TRANSPORT_ARTEFACT'
  AND ((flags & 32) != 0 OR (flags & 64) != 0 OR (flags & 8) != 0)
  AND price > 0 
  AND volume_real > 0;
