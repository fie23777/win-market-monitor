"""
Testes de Endurecimento da Fase B.1.1 — H002
Cobre os cenários de fronteira saturada, recuperação de crash, densidade > 5000 ticks,
recuperação de multiplicidade divergente e proteção fail-closed.
"""
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from src.config import load_config
from src.connection.mt5_reader import MT5QueryResult, MT5QueryStatus
from src.buffer.durable_spool import DurableSpool
from src.engine import MarketCollectorEngine
from src.reconciliation.tick_reconciler import TradeTick

TICK_FLAG_BUY = 32
TICK_FLAG_LAST = 1024
TICK_FLAGS_TRADE = TICK_FLAG_BUY | TICK_FLAG_LAST # 1056 (ou 1080)


def test_mono_temporal_density_exceeding_5000_triggers_inconclusive_and_fail_closed(tmp_path):
    """
    H002 B.1.1 Requisito 2 e 5:
    Quando a densidade de um único milissegundo excede max_batch_size (5000 ticks),
    o sistema registra intervalo inconclusivo, impede o avanço de checkpoint,
    e após stagnation_limit_cycles aciona estado de erro operacional fail-closed acionável.
    """
    db_file = str(tmp_path / "spool_fail_closed_dense.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool
    )
    engine._stagnation_limit_cycles = 3
    engine._max_batch_size = 5000
    engine._adaptive_batch_size = 5000

    # Gerar 5.001 ticks idênticos no mesmo milissegundo exato
    mono_msc = 1792573200500
    raw_ticks = [
        {
            "time": 1792573200,
            "time_msc": mono_msc,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080
        }
        for _ in range(5001)
    ]

    # H002 B.2 Condição 5: A população total no MT5 contém 5.001 registros,
    # mas a API chamada com requested_to_or_count=5000 devolve no máximo 5.000 registros (truncamento por capacidade)
    mock_reader.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=raw_ticks[:5000],
        requested_from=mono_msc,
        requested_to_or_count=5000,
        is_capacity_saturated=True
    )

    initial_ckp = spool.get_checkpoints("WINZ26").last_captured_msc
    assert engine.is_healthy() is True

    # Ciclo 1: Detecta densidade mono-temporal extrema no limite máximo (5000 ticks)
    inserted_1 = engine.capture_step()
    assert inserted_1 == 0
    # Checkpoint NÃO avança
    assert spool.get_checkpoints("WINZ26").last_captured_msc == initial_ckp
    assert len(engine.inconclusive_intervals) >= 1
    assert engine.inconclusive_intervals[0]["time_msc"] == mono_msc
    assert engine.inconclusive_intervals[0]["tick_count"] == 5000

    # Ciclo 2: Continua saturado
    engine.capture_step()
    assert engine.is_healthy() is True # Ainda dentro do limite de tolerância (2 < 3)

    # Ciclo 3: Atinge stagnation_limit_cycles=3 -> Aciona fail-closed operacional
    engine.capture_step()
    assert engine.operational_status == "CRITICAL_STAGNATION_LIMIT_EXCEEDED"
    assert engine.has_critical_stagnation is True
    assert engine.is_healthy() is False
    assert "Densidade extrema" in engine.last_stagnation_error

    # Ciclo 4: Em fail-closed, novas tentativas são bloqueadas
    res_4 = engine.capture_step()
    assert res_4 == 0
    assert spool.get_checkpoints("WINZ26").last_captured_msc == initial_ckp

    # H002 B.2 Condição 4: Comprovação de que o estado fail-closed SOBREVIVE A REINICIALIZAÇÃO DO PROCESSO
    del engine
    mock_reader_reboot = MagicMock()
    mock_reader_reboot.is_connected.return_value = True
    spool_reboot = DurableSpool(db_file)
    engine_reboot = MarketCollectorEngine(
        config=config,
        reader=mock_reader_reboot,
        spool=spool_reboot
    )
    # Alerta persiste mesmo após nova instância com memória limpa
    assert engine_reboot.operational_status == "CRITICAL_STAGNATION_LIMIT_EXCEEDED"
    assert engine_reboot.has_critical_stagnation is True
    assert engine_reboot.is_healthy() is False
    assert engine_reboot.capture_step() == 0


def test_process_crash_after_partial_boundary_and_cold_restart_recovers_without_loss_or_duplicates(tmp_path):
    """
    H002 B.1.1 Requisito 1 e 4:
    Interrupção após lote saturado. Demonstra que a retenção dos registros do milissegundo de
    fronteira garante que, após reinício a frio com memória vazia, a retomada não perde
    e nem duplica nenhum negócio economicamente idêntico.
    """
    db_file = str(tmp_path / "spool_crash_recovery.db")
    spool_proc1 = DurableSpool(db_file)
    mock_reader_proc1 = MagicMock()
    mock_reader_proc1.is_connected.return_value = True

    config = load_config()
    engine_proc1 = MarketCollectorEngine(
        config=config,
        reader=mock_reader_proc1,
        spool=spool_proc1
    )

    # Lote de 1.000 ticks saturado:
    # 900 ticks em 1792573200100 a 1792573200999 (milissegundos fechados)
    # 100 ticks na fronteira de corte 1792573201000
    batch_1 = []
    for i in range(900):
        batch_1.append({
            "time": 1792573200,
            "time_msc": 1792573200100 + i,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080
        })
    for i in range(100):
        batch_1.append({
            "time": 1792573201,
            "time_msc": 1792573201000,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 2.0,
            "volume_real": 2.0,
            "flags": 1080
        })

    mock_reader_proc1.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=batch_1,
        requested_from=1792573200100,
        requested_to_or_count=1000,
        is_capacity_saturated=True
    )

    inserted_proc1 = engine_proc1.capture_step()
    # Somente os 900 ticks da janela comprovada são gravados! Os 100 da fronteira incerta foram retidos.
    assert inserted_proc1 == 900
    ckp_saved = spool_proc1.get_checkpoints("WINZ26").last_captured_msc
    assert ckp_saved == 1792573200999

    # --- SIMULAÇÃO DE CRASH / MORTE DO PROCESSO ---
    del engine_proc1
    del spool_proc1
    del mock_reader_proc1

    # --- NOVO PROCESSO LIMPO (COLD START) ---
    spool_proc2 = DurableSpool(db_file)
    mock_reader_proc2 = MagicMock()
    mock_reader_proc2.is_connected.return_value = True

    engine_proc2 = MarketCollectorEngine(
        config=config,
        reader=mock_reader_proc2,
        spool=spool_proc2
    )

    # O novo processo lê os checkpoints do SQLite em disco:
    ckp_proc2 = spool_proc2.get_checkpoints("WINZ26")
    assert ckp_proc2.last_captured_msc == 1792573200999

    # A nova consulta traz os 100 ticks da fronteira 1792573201000 + 50 novos ticks em 1792573201050 (não saturada)
    batch_2 = batch_1[900:] # Os 100 ticks de 1792573201000
    for i in range(50):
        batch_2.append({
            "time": 1792573201,
            "time_msc": 1792573201050 + i,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 1.0,
            "volume_real": 1.0,
            "flags": 1080
        })

    mock_reader_proc2.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=batch_2,
        requested_from=1792573200999,
        requested_to_or_count=1000,
        is_capacity_saturated=False
    )

    inserted_proc2 = engine_proc2.capture_step()
    # Os 100 da fronteira + 50 novos são inseridos sem falha
    assert inserted_proc2 == 150

    # Total geral no buffer local SQLite:
    # 900 (do processo 1) + 150 (do processo 2) = 1.050 ticks
    pending = spool_proc2.get_pending_ticks(limit=2000)
    assert len(pending) == 1050
    assert spool_proc2.get_checkpoints("WINZ26").last_captured_msc == 1792573201099


def test_overlapping_batches_with_divergent_multiplicities(tmp_path):
    """
    H002 B.1.1 Requisito 4:
    Sobreposição de lotes com multiplicidades divergentes.
    Lote 1 traz 3 negócios idênticos no mesmo ms.
    Lote 2 (reprocessamento tardio) traz 5 negócios idênticos no mesmo ms.
    Comprova que os 3 primeiros são deduplicados e os 2 adicionais são persistidos.
    """
    db_file = str(tmp_path / "spool_divergent_mult.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool
    )

    msc = 1792573205000
    # Lote 1: 3 ticks economicamente idênticos
    batch_1 = [
        {
            "time": 1792573205,
            "time_msc": msc,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 5.0,
            "volume_real": 5.0,
            "flags": 1080
        }
        for _ in range(3)
    ]

    mock_reader.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=batch_1,
        requested_from=msc,
        requested_to_or_count=1000,
        is_capacity_saturated=False
    )
    inserted_1 = engine.capture_step()
    assert inserted_1 == 3

    # Lote 2: A bolsa atualizou o histórico e agora há 5 negócios idênticos no mesmo ms
    batch_2 = [
        {
            "time": 1792573205,
            "time_msc": msc,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 5.0,
            "volume_real": 5.0,
            "flags": 1080
        }
        for _ in range(5)
    ]

    mock_reader.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.SUCCESS_WITH_DATA,
        ticks=batch_2,
        requested_from=msc,
        requested_to_or_count=1000,
        is_capacity_saturated=False
    )
    inserted_2 = engine.capture_step()
    # Exatamente os 2 negócios adicionais são inseridos!
    assert inserted_2 == 2

    pending = spool.get_pending_ticks(limit=10)
    assert len(pending) == 5
    # Ordinais 1, 2, 3, 4, 5 preservados determinísticamente
    ordinals = [p.occurrence_ordinal for p in pending]
    assert ordinals == [1, 2, 3, 4, 5]


def test_inconclusive_interval_strictly_blocks_watermark_advance(tmp_path):
    """
    H002 B.1.1 Requisito 4:
    Consulta que retorna MT5_QUERY_ERROR ou INCONCLUSIVE
    não avança o checkpoint nem persiste nada.
    """
    db_file = str(tmp_path / "spool_inconclusive.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool
    )

    # Checkpoint pré-existente
    spool.update_captured_checkpoint(1792573200000, stream_key="WINZ26")
    ckp_before = spool.get_checkpoints("WINZ26").last_captured_msc

    mock_reader.query_ticks_from.return_value = MT5QueryResult(
        status=MT5QueryStatus.INCONCLUSIVE,
        ticks=[],
        error_code=4001,
        error_message="Broker internal synchronization timeout"
    )

    inserted = engine.capture_step()
    assert inserted == 0
    ckp_after = spool.get_checkpoints("WINZ26").last_captured_msc
    assert ckp_after == ckp_before


def test_durable_spool_total_changes_performance_and_accuracy(tmp_path):
    """
    H002 B.1.1 Requisito 3:
    Verifica a exatidão da contabilização O(1) via total_changes no SQLite,
    comprovando duplicate_count e inserted_count sob lotes mistos.
    """
    db_file = str(tmp_path / "spool_total_changes.db")
    spool = DurableSpool(db_file)

    now = datetime.now(timezone.utc)
    # Lote 1: 50 ticks novos
    ticks_batch_1 = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1792573200000 + i,
            time_utc=now,
            price=125000.0,
            volume_real=1.0,
            flags=1080,
            trade_side="BUY",
            occurrence_ordinal=1,
            provenance="REALTIME_STREAM"
        )
        for i in range(50)
    ]
    res_1 = spool.append_ticks_batch_detailed(ticks_batch_1)
    assert res_1.attempted_count == 50
    assert res_1.inserted_count == 50
    assert res_1.duplicate_count == 0

    # Lote 2: 50 ticks já existentes + 30 ticks novos = 80 attempted
    ticks_batch_2 = list(ticks_batch_1)
    for i in range(50, 80):
        ticks_batch_2.append(
            TradeTick(
                symbol="WINZ26",
                time_msc=1792573200000 + i,
                time_utc=now,
                price=125000.0,
                volume_real=1.0,
                flags=1080,
                trade_side="BUY",
                occurrence_ordinal=1,
                provenance="REALTIME_STREAM"
            )
        )
    res_2 = spool.append_ticks_batch_detailed(ticks_batch_2)
    assert res_2.attempted_count == 80
    assert res_2.inserted_count == 30
    assert res_2.duplicate_count == 50
