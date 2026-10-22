"""
Testes de Regressão e Validação da Fase B.2 — H002
Comprova o desacoplamento do Reconciliador Histórico de last_captured_msc,
a preservação estrita de W_complete (ACK PostgreSQL obrigatório),
o tratamento determinístico de falhas MT5/intervalos inconclusivos,
a recuperação após interrupção e a proteção sob janelas com saturação de capacidade.
"""
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

from src.buffer.durable_spool import DurableSpool
from src.reconciliation.periodic_reconciler import PeriodicHistoricalReconciler
from src.connection.mt5_reader import MT5QueryResult, MT5QueryStatus
from src.domain.models import TradeTick


def test_reconciler_advances_independently_when_capture_is_stagnated(tmp_path):
    """
    H002 B.2 Condição 1 e 6:
    Comprova que quando a captura realtime está completamente parada/estagnada
    (last_captured_msc estacionado em 1000000000), o reconciliador histórico
    audita e avança W_complete independentemente até o teto de mercado (now - safety_lag).
    """
    db_file = str(tmp_path / "spool_reconciler_decoupled.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    # Captura parada há muito tempo
    stagnated_captured_msc = 1792400400000 # 09:00 BRT
    spool.update_captured_checkpoint(stagnated_captured_msc, stream_key="WINZ26")
    ckp = spool.get_checkpoints("WINZ26")
    assert ckp.last_captured_msc == stagnated_captured_msc
    assert ckp.watermark_complete_msc == 0

    reconciler = PeriodicHistoricalReconciler(
        reader=mock_reader,
        spool=spool,
        symbol="WINZ26",
        safety_lag_ms=2000,
        max_window_ms=60000 # 1 minuto
    )

    # Horário atual de mercado 5 minutos à frente da captura parada
    now_server_msc = stagnated_captured_msc + 300000 # +5 min (1792400700000)

    # MT5 retorna ticks históricos verificados no intervalo auditado
    hist_ticks = [
        {
            "time": 1792400410,
            "time_msc": stagnated_captured_msc + 10000,
            "bid": 125000.0,
            "ask": 125005.0,
            "last": 125000.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080
        }
    ]
    expected_ceiling_msc = now_server_msc - 2000 # 1792400698000
    mock_reader.query_ticks_range.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=hist_ticks,
        requested_from=expected_ceiling_msc - 60000,
        requested_to_or_count=expected_ceiling_msc
    )

    # Função de replicação remota: simula envio e ACK imediato no PostgreSQL
    def mock_replicate_to_postgres():
        pending = spool.get_pending_ticks(limit=100)
        fps = [p.tick_fingerprint for p in pending]
        max_msc = max(p.time_msc for p in pending)
        spool.mark_ticks_replicated(fps, max_msc, stream_key="WINZ26")

    res = reconciler.reconcile_cycle(
        now_server_msc=now_server_msc,
        session_start_msc=stagnated_captured_msc,
        replicate_fn=mock_replicate_to_postgres
    )

    assert res["status"] == "RECONCILED"
    assert res["recovered_count"] == 1
    # W_complete avançou independentemente para o teto de auditoria de mercado (quase 5 minutos à frente da captura parada!)
    assert res["w_complete"] == expected_ceiling_msc
    assert res["w_complete"] > stagnated_captured_msc
    # last_captured_msc permaneceu inalterado (estritamente desacoplado)
    ckp_after = spool.get_checkpoints("WINZ26")
    assert ckp_after.last_captured_msc == stagnated_captured_msc
    assert ckp_after.watermark_complete_msc == expected_ceiling_msc


def test_reconciler_strictly_blocks_w_complete_when_ticks_remain_pending(tmp_path):
    """
    H002 B.2 Condição 2 e 6:
    Comprova que W_complete NÃO avança se negócios recuperados permanecerem
    com status 'PENDING' (sem confirmação/ACK do PostgreSQL).
    """
    db_file = str(tmp_path / "spool_pending_blocks_w_complete.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    start_msc = 1792400400000
    reconciler = PeriodicHistoricalReconciler(
        reader=mock_reader,
        spool=spool,
        symbol="WINZ26",
        safety_lag_ms=2000,
        max_window_ms=60000
    )

    hist_ticks = [
        {
            "time": 1792400420,
            "time_msc": start_msc + 20000,
            "bid": 125000.0,
            "ask": 125005.0,
            "last": 125000.0,
            "volume": 2.0,
            "volume_real": 2.0,
            "flags": 1080
        }
    ]
    mock_reader.query_ticks_range.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=hist_ticks,
        requested_from=start_msc,
        requested_to_or_count=start_msc + 60000
    )

    # Executar SEM replicate_fn: negócios ficam PENDING no spool
    res = reconciler.reconcile_cycle(
        now_server_msc=start_msc + 120000,
        session_start_msc=start_msc,
        replicate_fn=None
    )

    assert res["status"] == "RECOVERED_PENDING_REPLICATION"
    assert res["pending_replication"] is True
    assert res["recovered_count"] == 1
    # W_complete NÃO avançou!
    assert res["w_complete"] == 0
    assert spool.get_checkpoints("WINZ26").watermark_complete_msc == 0


def test_reconciler_rejects_mt5_failures_and_inconclusive_queries(tmp_path):
    """
    H002 B.2 Condição 3:
    Comprova que respostas com MT5_QUERY_ERROR, DISCONNECTED ou INCONCLUSIVE
    NUNCA são tratadas como UP_TO_DATE ou EMPTY_VERIFIED, retendo W_complete.
    """
    db_file = str(tmp_path / "spool_mt5_errors.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()

    start_msc = 1792400400000
    spool.update_complete_watermark(start_msc, stream_key="WINZ26")

    reconciler = PeriodicHistoricalReconciler(
        reader=mock_reader,
        spool=spool,
        symbol="WINZ26",
        safety_lag_ms=2000,
        max_window_ms=60000
    )

    # 1. MT5 Desconectado
    mock_reader.is_connected.return_value = False
    res_disc = reconciler.reconcile_cycle(now_server_msc=start_msc + 120000, session_start_msc=start_msc)
    assert res_disc["status"] == "SKIPPED_DISCONNECTED"
    assert spool.get_checkpoints("WINZ26").watermark_complete_msc == start_msc

    # 2. MT5 com erro de consulta (MT5_QUERY_ERROR)
    mock_reader.is_connected.return_value = True
    mock_reader.query_ticks_range.return_value = MT5QueryResult(
        status=MT5QueryStatus.MT5_QUERY_ERROR,
        ticks=[],
        error_code=4014,
        error_message="Terminal internal query timeout"
    )
    res_err = reconciler.reconcile_cycle(now_server_msc=start_msc + 120000, session_start_msc=start_msc)
    assert res_err["status"] == "INCONCLUSIVE"
    assert spool.get_checkpoints("WINZ26").watermark_complete_msc == start_msc

    # 3. MT5 com status INCONCLUSIVE
    mock_reader.query_ticks_range.return_value = MT5QueryResult(
        status=MT5QueryStatus.INCONCLUSIVE,
        ticks=[],
        error_message="Retorno indeterminado sem verificação de broker"
    )
    res_inc = reconciler.reconcile_cycle(now_server_msc=start_msc + 120000, session_start_msc=start_msc)
    assert res_inc["status"] == "INCONCLUSIVE"
    assert spool.get_checkpoints("WINZ26").watermark_complete_msc == start_msc


def test_reconciler_recovers_after_interruption_between_local_append_and_remote_ack(tmp_path):
    """
    H002 B.2 Condição 7:
    Simula falha/interrupção após persistência local no spool mas antes do ACK
    no PostgreSQL. No ciclo seguinte, com novo processo, o reconciliador detecta
    as pendências, conclui a replicação e somente então avança W_complete.
    """
    db_file = str(tmp_path / "spool_crash_ack.db")
    spool_1 = DurableSpool(db_file)
    mock_reader_1 = MagicMock()
    mock_reader_1.is_connected.return_value = True

    start_msc = 1792400400000
    reconciler_1 = PeriodicHistoricalReconciler(
        reader=mock_reader_1,
        spool=spool_1,
        symbol="WINZ26",
        safety_lag_ms=2000,
        max_window_ms=60000
    )

    hist_ticks = [
        {
            "time": 1792400415,
            "time_msc": start_msc + 15000,
            "bid": 125000.0,
            "ask": 125005.0,
            "last": 125000.0,
            "volume": 3.0,
            "volume_real": 3.0,
            "flags": 1080
        }
    ]
    mock_reader_1.query_ticks_range.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=hist_ticks,
        requested_from=start_msc,
        requested_to_or_count=start_msc + 60000
    )

    # Ciclo 1: Interrompe antes da replicação remota
    res_1 = reconciler_1.reconcile_cycle(
        now_server_msc=start_msc + 120000,
        session_start_msc=start_msc,
        replicate_fn=None
    )
    assert res_1["status"] == "RECOVERED_PENDING_REPLICATION"
    assert spool_1.get_checkpoints("WINZ26").watermark_complete_msc == 0

    # Simular morte do processo 1 e inicialização do processo 2
    del reconciler_1
    del spool_1
    del mock_reader_1

    spool_2 = DurableSpool(db_file)
    mock_reader_2 = MagicMock()
    mock_reader_2.is_connected.return_value = True
    reconciler_2 = PeriodicHistoricalReconciler(
        reader=mock_reader_2,
        spool=spool_2,
        symbol="WINZ26",
        safety_lag_ms=2000,
        max_window_ms=60000
    )
    mock_reader_2.query_ticks_range.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=hist_ticks,
        requested_from=start_msc,
        requested_to_or_count=start_msc + 60000
    )

    # Função de replicação ativada no processo 2
    def replicate_and_ack():
        pending = spool_2.get_pending_ticks(limit=100)
        fps = [p.tick_fingerprint for p in pending]
        spool_2.mark_ticks_replicated(fps, max(p.time_msc for p in pending), stream_key="WINZ26")

    res_2 = reconciler_2.reconcile_cycle(
        now_server_msc=start_msc + 120000,
        session_start_msc=start_msc,
        replicate_fn=replicate_and_ack
    )

    # Comprovação: Processo 2 não duplicou, confirmou durabilidade remota e avançou W_complete
    assert res_2["status"] == "RECONCILED"
    assert res_2["w_complete"] == start_msc + 60000
    assert spool_2.get_checkpoints("WINZ26").watermark_complete_msc == start_msc + 60000


def test_reconciler_capacity_saturation_recedes_to_safe_boundary(tmp_path):
    """
    H002 B.2 Condição 8:
    Comprova que quando query_ticks_range atinge a capacidade máxima (lote saturado)
    e a fronteira final possui ticks no mesmo milissegundo, W_complete recua
    para o maior milissegundo estritamente anterior, não declarando completude
    sobre um milissegundo potencialmente truncado.
    """
    db_file = str(tmp_path / "spool_range_saturation.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    start_msc = 1792400400000
    reconciler = PeriodicHistoricalReconciler(
        reader=mock_reader,
        spool=spool,
        symbol="WINZ26",
        safety_lag_ms=2000,
        max_window_ms=60000
    )

    # Lote de histórico saturado na capacidade:
    # 900 ticks em start_msc a start_msc + 50000
    # 100 ticks na fronteira saturada start_msc + 60000
    saturated_ticks = []
    for i in range(900):
        saturated_ticks.append({
            "time": 1792400400,
            "time_msc": start_msc + i * 50,
            "bid": 125000.0,
            "ask": 125005.0,
            "last": 125000.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080
        })
    frontier_msc = start_msc + 60000
    for _ in range(100):
        saturated_ticks.append({
            "time": 1792400460,
            "time_msc": frontier_msc,
            "bid": 125000.0,
            "ask": 125005.0,
            "last": 125000.0,
            "volume": 2.0,
            "volume_real": 2.0,
            "flags": 1080
        })

    mock_reader.query_ticks_range.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=saturated_ticks,
        requested_from=start_msc,
        requested_to_or_count=frontier_msc,
        is_capacity_saturated=True
    )

    def replicate_and_ack():
        pending = spool.get_pending_ticks(limit=2000)
        fps = [p.tick_fingerprint for p in pending]
        if fps:
            spool.mark_ticks_replicated(fps, max(p.time_msc for p in pending), stream_key="WINZ26")

    res = reconciler.reconcile_cycle(
        now_server_msc=start_msc + 120000,
        session_start_msc=start_msc,
        replicate_fn=replicate_and_ack
    )

    assert res["status"] == "RECONCILED"
    # W_complete recua para o maior ms estritamente anterior à fronteira saturada (start_msc + 899 * 50 = start_msc + 44950)
    expected_safe_msc = start_msc + 899 * 50
    assert res["w_complete"] == expected_safe_msc
    assert res["w_complete"] < frontier_msc
    assert spool.get_checkpoints("WINZ26").watermark_complete_msc == expected_safe_msc
