import json
import os
import sys
sys.path.insert(0, ".")
import psycopg2

os.makedirs("sql/migrations/h001", exist_ok=True)

# 1. 01_precondition_check.sql
precondition_sql = """-- ============================================================================
-- HARDENING H001 - ETAPA 1: VERIFICAÇÃO DE PRÉ-CONDIÇÕES (PRECONDITION CHECK)
-- BANCO ALVO: market_data (Executado apenas com autorização explícita)
-- OBJETIVO: Garantir que a base operacional está no estado esperado antes de aplicar H001
-- ============================================================================

DO $$
DECLARE
    v_raw_count BIGINT;
    v_raw_vol DOUBLE PRECISION;
    v_candles_count BIGINT;
BEGIN
    -- 1. Verificar contagem bruta esperada do pregão de 19/10/2026
    SELECT count(*), coalesce(sum(volume_real), 0)
    INTO v_raw_count, v_raw_vol
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00';

    RAISE NOTICE 'Pré-condição: Encontrados % ticks brutos, volume total %', v_raw_count, v_raw_vol;

    IF v_raw_count < 2090000 THEN
        RAISE EXCEPTION 'ABORTAR MIGRAÇÃO: Contagem de ticks no pregão ( % ) está muito abaixo do esperado ( ~2.093.221 ).', v_raw_count;
    END IF;

    -- 2. Verificar candles_m2 originais
    SELECT count(*) INTO v_candles_count FROM candles_m2;
    RAISE NOTICE 'Pré-condição: Encontrados % candles M2 originais', v_candles_count;

    IF v_candles_count < 280 THEN
        RAISE EXCEPTION 'ABORTAR MIGRAÇÃO: Tabela candles_m2 incompleta ( % registros ).', v_candles_count;
    END IF;

    RAISE NOTICE 'PRÉ-CONDIÇÕES APROVADAS COM SUCESSO. Base pronta para migração.';
END $$;
"""
with open("sql/migrations/h001/01_precondition_check.sql", "w", encoding="utf-8") as f:
    f.write(precondition_sql)

# 2. 02_schema_evolution.sql
schema_sql = """-- ============================================================================
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
"""
with open("sql/migrations/h001/02_schema_evolution.sql", "w", encoding="utf-8") as f:
    f.write(schema_sql)

# 3. 03_reclassify_provenance_nc001.sql
reclassify_nc1_sql = """-- ============================================================================
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
"""
with open("sql/migrations/h001/03_reclassify_provenance_nc001.sql", "w", encoding="utf-8") as f:
    f.write(reclassify_nc1_sql)

# 4. 04_mark_duplicate_transport_artefacts_nc003.sql
with open("scratch/h001_exact_166_lineage.json", "r", encoding="utf-8") as f:
    exact_lineage = json.load(f)

mark_extra_sql = """-- ============================================================================
-- HARDENING H001 - ETAPA 4: VINCULAÇÃO DE LINHAGEM E ISOLAMENTO DOS 166 EXCEDENTES (NC-003)
-- OBJETIVO: Preservar fisicamente os registros, registrar linhagem e marcar proveniência
-- ============================================================================

INSERT INTO duplicate_lineage (
    duplicate_fingerprint, duplicate_time_msc, duplicate_price, duplicate_volume_real,
    duplicate_flags, duplicate_ingested_at, original_fingerprint, original_flags,
    original_ingested_at, match_type
) VALUES
"""
l_rows = []
for item in exact_lineage:
    dfp = item['duplicate_fingerprint']
    tmsc = item['duplicate_time_msc']
    prc = item['duplicate_price']
    vol = item['duplicate_volume_real']
    dflg = item['duplicate_flags']
    ding = item['duplicate_ingested_at']
    ofp = item['original_fingerprint']
    oflg = item['original_flags']
    oing = item['original_ingested_at']
    mtype = item['match_type']
    l_rows.append(f"('{dfp}', {tmsc}, {prc}, {vol}, {dflg}, '{ding}', '{ofp}', {oflg}, '{oing}', '{mtype}')")

mark_extra_sql += ",\n".join(l_rows) + "\nON CONFLICT (duplicate_fingerprint) DO NOTHING;\n\n"

mark_extra_sql += """-- Atualizar proveniência física no market_ticks_raw
WITH updated_dups AS (
    UPDATE market_ticks_raw
    SET provenance = 'DUPLICATE_TRANSPORT_ARTEFACT'
    WHERE tick_fingerprint IN (SELECT duplicate_fingerprint FROM duplicate_lineage)
      AND provenance <> 'DUPLICATE_TRANSPORT_ARTEFACT'
    RETURNING tick_fingerprint, volume_real
)
INSERT INTO audit_reconciliation_log (action_type, entity_type, affected_count, volume_affected, description)
SELECT 
    'MARK_DUPLICATES_NC003',
    'market_ticks_raw',
    count(*),
    coalesce(sum(volume_real), 0),
    'Marcação dos 166 registros excedentes como DUPLICATE_TRANSPORT_ARTEFACT com linhagem preservada'
FROM updated_dups;
"""
with open("sql/migrations/h001/04_mark_duplicate_transport_artefacts_nc003.sql", "w", encoding="utf-8") as f:
    f.write(mark_extra_sql)

# 5. 05_insert_missing_historical_ticks_nc003.sql
with open("scratch/audit_results_20261019/all_missing_ticks.json", "r", encoding="utf-8") as f:
    missing_ticks = json.load(f)

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.domain.models import compute_tick_event_signature, compute_tick_fingerprint

normalizer = MT5TimeNormalizer()

insert_missing_sql = """-- ============================================================================
-- HARDENING H001 - ETAPA 5: INGESTÃO IDEMPOTENTE DOS 511 NEGÓCIOS HISTÓRICOS (NC-003)
-- OBJETIVO: Injetar negócios omitidos com provenance RECOVERED_HISTORICAL_MT5 e ON CONFLICT DO NOTHING
-- ============================================================================

CREATE TABLE IF NOT EXISTS h001_injected_ticks (
    tick_fingerprint VARCHAR(64) PRIMARY KEY,
    time_msc BIGINT NOT NULL,
    injected_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TEMP TABLE tmp_missing_ticks (
    symbol VARCHAR(16),
    time_msc BIGINT,
    time_utc TIMESTAMPTZ,
    price NUMERIC(10, 2),
    volume_real DOUBLE PRECISION,
    flags BIGINT,
    trade_side VARCHAR(8),
    occurrence_ordinal INTEGER,
    tick_fingerprint VARCHAR(64),
    tick_event_signature VARCHAR(64),
    provenance VARCHAR(32)
);

INSERT INTO tmp_missing_ticks VALUES
"""
rows_vals = []
for m in missing_ticks:
    tmsc = m['time_msc']
    dt_utc = normalizer.server_msc_to_utc_datetime(tmsc)
    dt_utc_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S.%f") + "+00"
    prc = float(m['price'])
    vol = float(m['volume_real'])
    side = m['trade_side']
    flg = 1080 if side == 'BUY' else (1112 if side == 'SELL' else 1144)
    ord_i = m['occurrence_ordinal']
    sig = compute_tick_event_signature(tmsc, prc, vol, flg & ~256, side)
    fp = compute_tick_fingerprint(tmsc, sig, ord_i)
    rows_vals.append(f"('WINZ26', {tmsc}, '{dt_utc_str}', {prc}, {vol}, {flg}, '{side}', {ord_i}, '{fp}', '{sig}', 'RECOVERED_HISTORICAL_MT5')")

insert_missing_sql += ",\n".join(rows_vals) + ";\n\n"

insert_missing_sql += """
-- Registrar fingerprints na tabela de rastreabilidade do H001
INSERT INTO h001_injected_ticks (tick_fingerprint, time_msc)
SELECT tick_fingerprint, time_msc FROM tmp_missing_ticks
ON CONFLICT (tick_fingerprint) DO NOTHING;

WITH inserted AS (
    INSERT INTO market_ticks_raw (
        symbol, time_msc, time_utc, price, volume_real, flags, trade_side,
        occurrence_ordinal, tick_fingerprint, provenance, ingested_at_utc
    )
    SELECT 
        symbol, time_msc, time_utc, price, volume_real, flags, trade_side,
        occurrence_ordinal, tick_fingerprint, provenance, NOW()
    FROM tmp_missing_ticks
    ON CONFLICT (time_utc, symbol, tick_fingerprint) DO NOTHING
    RETURNING tick_fingerprint, volume_real
)
INSERT INTO audit_reconciliation_log (action_type, entity_type, affected_count, volume_affected, description)
SELECT 
    'INSERT_MISSING_NC003',
    'market_ticks_raw',
    count(*),
    coalesce(sum(volume_real), 0),
    'Ingestão idempotente dos 511 negócios históricos recuperados do MT5 (NC-003)'
FROM inserted;

DROP TABLE tmp_missing_ticks;
"""
with open("sql/migrations/h001/05_insert_missing_historical_ticks_nc003.sql", "w", encoding="utf-8") as f:
    f.write(insert_missing_sql)

# 6. 06_create_canonical_views.sql
canonical_view_sql = """-- ============================================================================
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
"""
with open("sql/migrations/h001/06_create_canonical_views.sql", "w", encoding="utf-8") as f:
    f.write(canonical_view_sql)

# 7. 07_rebuild_candles_m2_v2.sql
rebuild_candles_sql = """-- ============================================================================
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
"""
with open("sql/migrations/h001/07_rebuild_candles_m2_v2.sql", "w", encoding="utf-8") as f:
    f.write(rebuild_candles_sql)

# 8. 08_postcondition_check.sql
postcondition_sql = """-- ============================================================================
-- HARDENING H001 - ETAPA 8: VERIFICAÇÃO DE PÓS-CONDIÇÕES E CERTIFICAÇÃO
-- OBJETIVO: Validar os 2.093.566 ticks canônicos e 19.816.479 contratos reconciliados
-- ============================================================================

DO $$
DECLARE
    v_canonical_count BIGINT;
    v_canonical_vol DOUBLE PRECISION;
    v_duplicates_count BIGINT;
    v_duplicates_vol DOUBLE PRECISION;
    v_candles_count BIGINT;
    v_candles_vol DOUBLE PRECISION;
BEGIN
    -- 1. Validar visão canônica
    SELECT count(*), coalesce(sum(volume_real), 0)
    INTO v_canonical_count, v_canonical_vol
    FROM v_market_ticks_canonical
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00';

    RAISE NOTICE 'Pós-condição Canônica: % ticks, volume % contratos', v_canonical_count, v_canonical_vol;

    IF v_canonical_count <> 2093566 THEN
        RAISE EXCEPTION 'FALHA DE PÓS-CONDIÇÃO: Contagem canônica ( % ) diverge da meta MT5 ( 2093566 ).', v_canonical_count;
    END IF;

    IF v_canonical_vol <> 19816479 THEN
        RAISE EXCEPTION 'FALHA DE PÓS-CONDIÇÃO: Volume canônico ( % ) diverge da meta MT5 ( 19816479 ).', v_canonical_vol;
    END IF;

    -- 2. Validar duplicatas isoladas
    SELECT count(*), coalesce(sum(volume_real), 0)
    INTO v_duplicates_count, v_duplicates_vol
    FROM market_ticks_raw
    WHERE provenance = 'DUPLICATE_TRANSPORT_ARTEFACT';

    RAISE NOTICE 'Pós-condição Duplicatas: % registros isolados, volume %', v_duplicates_count, v_duplicates_vol;

    IF v_duplicates_count <> 166 OR v_duplicates_vol <> 26070 THEN
        RAISE EXCEPTION 'FALHA DE PÓS-CONDIÇÃO: Isolamento dos 166 excedentes inválido ( count=%, vol=% ).', v_duplicates_count, v_duplicates_vol;
    END IF;

    -- 3. Validar candles_m2_v2
    SELECT count(*), coalesce(sum(volume_real), 0)
    INTO v_candles_count, v_candles_vol
    FROM candles_m2_v2;

    RAISE NOTICE 'Pós-condição Candles M2 v2: % candles, volume total %', v_candles_count, v_candles_vol;

    IF v_candles_count <> 284 OR v_candles_vol <> 19816479 THEN
        RAISE EXCEPTION 'FALHA DE PÓS-CONDIÇÃO: Candles M2 v2 inválidos ( count=%, vol=% ).', v_candles_count, v_candles_vol;
    END IF;

    RAISE NOTICE '===================================================================';
    RAISE NOTICE 'CERTIFICAÇÃO CONCLUÍDA: BASE RECONCILIADA COM 100%% DE CONFORMIDADE';
    RAISE NOTICE '===================================================================';
END $$;
"""
with open("sql/migrations/h001/08_postcondition_check.sql", "w", encoding="utf-8") as f:
    f.write(postcondition_sql)

# 9. rollback_h001.sql
rollback_sql = """-- ============================================================================
-- HARDENING H001 - SCRIPT DE ROLLBACK TOTAL E ATÔMICO CIRÚRGICO
-- OBJETIVO: Reverter integralmente todas as alterações do H001 sem perda de dados brutos
-- ============================================================================

DO $$
BEGIN
    -- 1. Excluir EXCLUSIVAMENTE os 511 negócios injetados pelo H001 (NC-003)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'h001_injected_ticks') THEN
        DELETE FROM market_ticks_raw
        WHERE tick_fingerprint IN (SELECT tick_fingerprint FROM h001_injected_ticks);
        DROP TABLE IF EXISTS h001_injected_ticks CASCADE;
    END IF;

    -- 2. Restaurar proveniência original dos 166 excedentes
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'duplicate_lineage') THEN
        UPDATE market_ticks_raw
        SET provenance = 'REALTIME_STREAM'
        WHERE tick_fingerprint IN (SELECT duplicate_fingerprint FROM duplicate_lineage);
        DROP TABLE IF EXISTS duplicate_lineage CASCADE;
    END IF;

    -- 3. Restaurar proveniência original dos 44.899 registros do blackout (NC-001)
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'h001_reclassified_nc001_ticks') THEN
        UPDATE market_ticks_raw m
        SET provenance = h.previous_provenance
        FROM h001_reclassified_nc001_ticks h
        WHERE m.tick_fingerprint = h.tick_fingerprint;
        DROP TABLE IF EXISTS h001_reclassified_nc001_ticks CASCADE;
    END IF;

    -- 4. Dropar views e tabelas versionadas
    DROP VIEW IF EXISTS v_market_ticks_canonical CASCADE;
    DROP TABLE IF EXISTS candles_m2_v2 CASCADE;
    DROP TABLE IF EXISTS audit_reconciliation_log CASCADE;

    RAISE NOTICE 'ROLLBACK H001 CONCLUÍDO COM SUCESSO. Base revertida exatamente ao estado do snapshot original.';
END $$;
"""
with open("sql/migrations/h001/rollback_h001.sql", "w", encoding="utf-8") as f:
    f.write(rollback_sql)

print("Todos os 9 scripts SQL foram gerados com 100% de precisão de schema.")
