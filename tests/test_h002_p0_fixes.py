import os
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import pytest

from src.connection.mt5_reader import MT5ReadOnlyReader, MT5QueryStatus, MT5QueryResult
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.engine import MarketCollectorEngine
from src.config import load_config
from src.buffer.durable_spool import DurableSpool
from src.domain.models import TradeTick

def test_mt5_query_result_contract_distinguishes_empty_from_error():
    """
    H002-P0-03 Correção:
    Garante que MT5QueryResult diferencia com precisão:
    - SUCCESS_EMPTY_VERIFIED (terminal conectado, last_error=0, lista vazia confirmada)
    - MT5_QUERY_ERROR (copy_ticks retornando None com código de erro)
    - INCONCLUSIVE (copy_ticks retornando None sem código de erro explícito)
    - DISCONNECTED (terminal desconectado)
    """
    normalizer = MT5TimeNormalizer()
    reader = MT5ReadOnlyReader(time_normalizer=normalizer)
    
    with patch("src.connection.mt5_reader.mt5") as mock_mt5:
        # 1. DISCONNECTED
        reader.connected = False
        mock_mt5.terminal_info.return_value = MagicMock(connected=False)
        res_disc = reader.query_ticks_from("WINZ26", 1792573200000, 1000)
        assert res_disc.status == MT5QueryStatus.DISCONNECTED
        assert res_disc.ticks == []
        
        # 2. MT5_QUERY_ERROR (retorna None com erro MT5)
        reader.connected = True
        mock_mt5.terminal_info.return_value = MagicMock(connected=True)
        mock_mt5.copy_ticks_from.return_value = None
        mock_mt5.last_error.return_value = (-10004, "IPC timeout error")
        
        res_err = reader.query_ticks_from("WINZ26", 1792573200000, 1000)
        assert res_err.status == MT5QueryStatus.MT5_QUERY_ERROR
        assert res_err.error_code == -10004
        assert "IPC timeout error" in res_err.error_message
        
        # 3. INCONCLUSIVE (retorna None sem last_error)
        mock_mt5.copy_ticks_from.return_value = None
        mock_mt5.last_error.return_value = (0, "Success")
        res_inconcl = reader.query_ticks_from("WINZ26", 1792573200000, 1000)
        assert res_inconcl.status == MT5QueryStatus.INCONCLUSIVE
        
        # 4. SUCCESS_EMPTY_VERIFIED (retorna array vazio com last_error=0 e conectado)
        mock_mt5.copy_ticks_from.return_value = []
        mock_mt5.last_error.return_value = (0, "Success")
        res_empty = reader.query_ticks_from("WINZ26", 1792573200000, 1000)
        assert res_empty.status == MT5QueryStatus.SUCCESS_EMPTY_VERIFIED
        assert res_empty.ticks == []

def test_subsecond_resolution_preserved_in_cursor():
    """
    H002-P0-01 Correção:
    Garante que o cursor temporal preserva milissegundos e microssegundos no MT5ReadOnlyReader.
    """
    normalizer = MT5TimeNormalizer()
    reader = MT5ReadOnlyReader(time_normalizer=normalizer)
    
    t_msc = 1792573215789 # 09:00:15.789 BRT
    resolved_dt = reader._resolve_query_datetime(t_msc)
    
    # Validação: os 789 ms devem ser preservados como 789.000 microssegundos!
    assert resolved_dt.microsecond == 789000
    assert resolved_dt.timestamp() == 1792573215.789

def test_dense_boundary_multiple_ticks_same_millisecond_safe_watermark(tmp_path):
    """
    H002-P0-01 Correção:
    Quando um lote de 1.000 ticks atinge a capacidade máxima (saturação de batch)
    e a fronteira final possui múltiplos ticks no mesmo milissegundo,
    o checkpoint de captura NÃO deve avançar sobre o milissegundo saturado,
    mas sim fixar em safe_msc (maior milissegundo estritamente anterior à fronteira).
    """
    db_file = str(tmp_path / "spool_dense_test.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True
    
    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool
    )
    
    # Gerar lote saturado de 1.000 ticks:
    # 950 ticks em 1792573200100 a 1792573200950
    # 50 ticks exatamente no milissegundo de fronteira: 1792573201000
    raw_ticks = []
    for i in range(950):
        raw_ticks.append({
            "time": 1792573200,
            "time_msc": 1792573200000 + i,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080 # BUY + LAST
        })
    for _ in range(50):
        raw_ticks.append({
            "time": 1792573201,
            "time_msc": 1792573201000, # Fronteira densa
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 2.0,
            "volume_real": 2.0,
            "flags": 1080
        })
        
    mock_reader.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=raw_ticks,
        requested_from=1792573200000,
        requested_to_or_count=1000,
        is_capacity_saturated=True
    )
    
    inserted = engine.capture_step()
    # H002 B.1.1: Persistir apenas a porção comprovadamente completa (950 ticks)
    # Os 50 ticks do milissegundo de fronteira saturada são retidos para reprocessamento limpo
    assert inserted == 950
    
    ckp = spool.get_checkpoints("WINZ26")
    # O checkpoint seguro NÃO pode ser 1792573201000 (pois este foi cortado pela capacidade),
    # deve ser o maior milissegundo anterior à fronteira: 1792573200949!
    assert ckp.last_captured_msc == 1792573200949
    assert ckp.last_captured_msc < 1792573201000

    # Ciclo 2: Próxima consulta traz os 50 ticks restantes do milissegundo 1792573201000 (sem saturação)
    remaining_ticks = raw_ticks[950:]
    mock_reader.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=remaining_ticks,
        requested_from=1792573200950,
        requested_to_or_count=1000,
        is_capacity_saturated=False
    )
    inserted_2 = engine.capture_step()
    assert inserted_2 == 50
    ckp_2 = spool.get_checkpoints("WINZ26")
    assert ckp_2.last_captured_msc == 1792573201000

def test_checkpoint_advances_by_proven_coverage_even_with_existing_duplicates(tmp_path):
    """
    Ressalva 1 do Usuário:
    Checkpoints devem avançar por cobertura temporal comprovada e durabilidade confirmada,
    e NÃO unicamente pela quantidade de novos registros inseridos.
    Se um lote contém 990 registros já persistidos e 10 novos, e não saturou a capacidade,
    o checkpoint avança até a fronteira temporal comprovada do lote.
    """
    db_file = str(tmp_path / "spool_coverage_test.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True
    
    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool
    )
    
    # Inserir previamente 990 ticks via reconciliador
    pre_raw = [
        {
            "time": 1792573200,
            "time_msc": 1792573200000 + i,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080
        }
        for i in range(990)
    ]
    reconciled_pre = engine.reconciler.reconcile_raw_ticks(pre_raw, "WINZ26")
    spool.append_ticks_batch(reconciled_pre, stream_key="WINZ26")
    
    # Agora simular consulta retornando os 990 já existentes + 10 novos (total 1.000)
    # Mas configurado com batch_size de 2.000 para NÃO estar saturado (lote completo e exaurido)
    batch = []
    for i in range(1000):
        batch.append({
            "time": 1792573200,
            "time_msc": 1792573200000 + i,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080
        })
        
    mock_reader.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=batch,
        requested_from=1792573200000,
        requested_to_or_count=2000,
        is_capacity_saturated=False # Não saturado: janela totalmente percorrida!
    )
    
    inserted = engine.capture_step()
    # Apenas 10 eram novos
    assert inserted == 10
    
    ckp = spool.get_checkpoints("WINZ26")
    # O checkpoint avança até 1792573200999 (cobertura total comprovada dos 1.000 ticks)
    assert ckp.last_captured_msc == 1792573200999

def test_ultra_dense_millisecond_triggers_adaptive_expansion_and_telemetry(tmp_path):
    """
    H002-P0-01 Correção:
    Quando todos os ticks de um lote saturado ocorrem no mesmo milissegundo,
    o engine detecta o milissegundo ultra-denso, expande adaptive_batch_size
    e não avança o checkpoint prematuramente.
    """
    db_file = str(tmp_path / "spool_ultra_dense.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True
    
    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool
    )
    
    initial_batch_size = engine._adaptive_batch_size
    assert initial_batch_size == 1000
    
    # 1.000 ticks no mesmo milissegundo exato
    dense_ticks = [
        {
            "time": 1792573200,
            "time_msc": 1792573200500,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080
        }
        for _ in range(1000)
    ]
    
    mock_reader.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=dense_ticks,
        requested_from=1792573200000,
        requested_to_or_count=1000,
        is_capacity_saturated=True
    )
    
    engine.capture_step()
    
    # Comprovação: batch size foi expandido de 1000 para 2000!
    assert engine._adaptive_batch_size == 2000
    assert engine._cursor_stagnation_count == 1
    # Checkpoint NÃO avançou além do milissegundo não exaurido
    ckp = spool.get_checkpoints("WINZ26")
    assert ckp.last_captured_msc == 0

def test_continuous_pagination_over_2500_ticks_within_1_second(tmp_path):
    """
    H002-P0-01 Teste Obrigatório:
    Mais de 1.000 ticks dentro de 1 segundo (2.500 ticks).
    Demonstra que o coletor pagina progressivamente sem loops infinitos,
    sem descartar nenhum tick e preservando a multiplicidade exata.
    """
    db_file = str(tmp_path / "spool_2500_ticks.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True
    
    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool
    )
    
    session_start = engine._get_session_start_msc()
    session_sec = session_start // 1000

    # Criar 2.500 ticks dentro de 1 segundo a partir de session_start
    all_raw_ticks = []
    for i in range(2500):
        t_msc = session_start + int(i * 0.4)
        all_raw_ticks.append({
            "time": session_sec,
            "time_msc": t_msc,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": float(1 + (i % 5)),
            "volume_real": float(1 + (i % 5)),
            "flags": 1080
        })
        
    call_idx = 0
    def mock_query(sym, from_time, count):
        nonlocal call_idx
        # Filtrar a partir de from_time
        matching = [t for t in all_raw_ticks if t["time_msc"] >= from_time]
        slice_ticks = matching[:count]
        call_idx += 1
        return MT5QueryResult(
            status=MT5QueryStatus.SUCCESS_WITH_DATA if slice_ticks else MT5QueryStatus.SUCCESS_EMPTY_VERIFIED,
            ticks=slice_ticks,
            requested_from=from_time,
            requested_to_or_count=count,
            is_capacity_saturated=bool(len(slice_ticks) >= count)
        )
        
    mock_reader.query_ticks_from.side_effect = mock_query
    
    total_inserted = 0
    max_steps = 10
    step = 0
    while step < max_steps:
        step += 1
        ins = engine.capture_step()
        total_inserted += ins
        ckp = spool.get_checkpoints("WINZ26")
        if ckp.last_captured_msc >= all_raw_ticks[-1]["time_msc"]:
            break
            
    # Comprovação cabal: Todos os 2.500 ticks foram persistidos no spool sem perda!
    with spool._get_connection() as conn:
        count = conn.execute("SELECT count(*) FROM spool_ticks;").fetchone()[0]
        assert count == 2500, f"Esperado 2500 ticks, obtido {count}!"
        
    assert total_inserted == 2500
    assert ckp.last_captured_msc == all_raw_ticks[-1]["time_msc"]

def test_reconnection_during_pagination_recovers_gracefully(tmp_path):
    """
    H002-P0-01 Teste Obrigatório:
    Reconexão durante paginação.
    Demonstra recuperação graciosa quando o terminal desconecta temporariamente.
    """
    db_file = str(tmp_path / "spool_reconn.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    
    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool
    )
    
    # Simular terminal aparentemente conectado antes do envio, mas consulta retorna DISCONNECTED
    mock_reader.is_connected.return_value = True
    with patch.object(engine, "_attempt_mt5_reconnect", return_value=False) as mock_rec:
        mock_reader.query_ticks_from.return_value = MT5QueryResult(
            status=MT5QueryStatus.DISCONNECTED,
            ticks=[]
        )
        res = engine.capture_step()
        assert res == 0
        mock_rec.assert_called_once()
