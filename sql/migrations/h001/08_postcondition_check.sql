-- ============================================================================
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
