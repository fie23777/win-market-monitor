-- ============================================================================
-- HARDENING H001 - ETAPA 2: EVOLUÇÃO DE SCHEMA E GOVERNANÇA DE LINHAGEM
-- OBJETIVO: Criar tabelas de auditoria, linhagem de duplicatas e candles_m2_v2
-- ============================================================================

-- 1. Trilha de Auditoria e Reconciliação
CREATE TABLE IF NOT EXISTS audit_reconciliation_log (
    log_id BIGSERIAL PRIMARY KEY,
    execution_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action_type VARCHAR(64) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,
    affected_count BIGINT NOT NULL,
    volume_affected DOUBLE PRECISION NOT NULL,
    description TEXT NOT NULL
);

-- 2. Tabela de Linhagem Permanente de Duplicatas (preservação física)
CREATE TABLE IF NOT EXISTS duplicate_lineage (
    duplicate_fingerprint VARCHAR(64) PRIMARY KEY,
    duplicate_time_msc BIGINT NOT NULL,
    duplicate_price DOUBLE PRECISION NOT NULL,
    duplicate_volume_real DOUBLE PRECISION NOT NULL,
    duplicate_flags INTEGER NOT NULL,
    duplicate_ingested_at TIMESTAMPTZ NOT NULL,
    original_fingerprint VARCHAR(64) NOT NULL,
    original_flags INTEGER NOT NULL,
    original_ingested_at TIMESTAMPTZ NOT NULL,
    match_type VARCHAR(32) NOT NULL,
    linked_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_lineage_duplicate_msc ON duplicate_lineage(duplicate_time_msc);
CREATE INDEX IF NOT EXISTS idx_lineage_original_fp ON duplicate_lineage(original_fingerprint);

-- 3. Tabela Versionada de Candles M2 (candles_m2_v2)
CREATE TABLE IF NOT EXISTS candles_m2_v2 (
    candle_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    open_time_utc TIMESTAMPTZ NOT NULL,
    close_time_utc TIMESTAMPTZ NOT NULL,
    open NUMERIC(10, 2) NOT NULL,
    high NUMERIC(10, 2) NOT NULL,
    low NUMERIC(10, 2) NOT NULL,
    close NUMERIC(10, 2) NOT NULL,
    volume_real DOUBLE PRECISION NOT NULL,
    tick_volume BIGINT NOT NULL,
    spread INTEGER NOT NULL DEFAULT 5,
    state VARCHAR(16) NOT NULL DEFAULT 'REVISED',
    revision_number INTEGER NOT NULL DEFAULT 1,
    has_subsecond_ambiguity BOOLEAN NOT NULL DEFAULT TRUE,
    derived_from_provenance VARCHAR(64) NOT NULL DEFAULT 'CANONICAL_VIEW_V1',
    recalculated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_candle_v2_symbol_opentime UNIQUE (symbol, open_time_utc)
);

CREATE INDEX IF NOT EXISTS idx_candles_m2_v2_open ON candles_m2_v2(symbol, open_time_utc);
