-- ============================================================================
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
