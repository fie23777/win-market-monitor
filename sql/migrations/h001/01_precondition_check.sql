-- ============================================================================
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
