import pytest
from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
from src.reconciliation.tick_reconciler import (
    TickReconciler,
    TICK_FLAG_BID,
    TICK_FLAG_ASK,
    TICK_FLAG_LAST,
    TICK_FLAG_VOLUME,
    TICK_FLAG_BUY,
    TICK_FLAG_SELL
)
from src.engine import MarketCollectorEngine
from src.config import load_config
from src.buffer.durable_spool import DurableSpool
from src.domain.models import Checkpoints

def test_session_start_calculation_anchors_at_0900_brt():
    """
    Comprova que a primeira execução sem checkpoint ancora no início da sessão (09:00 BRT = 12:00 UTC),
    e NUNCA na época de 1970.
    """
    config = load_config()
    engine = MarketCollectorEngine(config=config, reader=MagicMock(), spool=MagicMock(), writer=MagicMock())

    target_date = date(2026, 10, 19)
    session_msc = engine._get_session_start_msc(target_date=target_date)
    session_dt = datetime.fromtimestamp(session_msc / 1000.0, tz=timezone.utc)

    # 19/10/2026 09:00 BRT corresponde a 19/10/2026 12:00:00 UTC
    assert session_dt.year == 2026
    assert session_dt.month == 10
    assert session_dt.day == 19
    assert session_dt.hour == 12
    assert session_dt.minute == 0
    assert session_dt.second == 0
    assert session_msc > 0

def test_book_only_ticks_are_completely_excluded():
    """
    Comprova que ticks de atualização exclusiva de BID/ASK são sumariamente descartados
    e nunca convertidos em TradeTicks de negócio com preço ou volume zero.
    """
    reconciler = TickReconciler()
    t_msc = 1792411200000

    raw_book_ticks = [
        # Atualização exclusiva de BID
        {"time": 1792411200, "time_msc": t_msc, "bid": 125000.0, "ask": 125005.0, "last": 0.0, "volume_real": 0.0, "flags": TICK_FLAG_BID},
        # Atualização exclusiva de ASK
        {"time": 1792411200, "time_msc": t_msc + 10, "bid": 125000.0, "ask": 125010.0, "last": 0.0, "volume_real": 0.0, "flags": TICK_FLAG_ASK},
        # Atualização de spread (BID + ASK)
        {"time": 1792411200, "time_msc": t_msc + 20, "bid": 124995.0, "ask": 125005.0, "last": 0.0, "volume_real": 0.0, "flags": TICK_FLAG_BID | TICK_FLAG_ASK},
        # Preço ou volume inválido mesmo com flag LAST
        {"time": 1792411200, "time_msc": t_msc + 30, "last": 0.0, "volume_real": 10.0, "flags": TICK_FLAG_LAST},
        {"time": 1792411200, "time_msc": t_msc + 40, "last": 125000.0, "volume_real": 0.0, "flags": TICK_FLAG_LAST},
    ]

    result = reconciler.reconcile_raw_ticks(raw_book_ticks, symbol="WINZ26")
    assert len(result) == 0, "Ticks de book foram indevidamente persistidos!"

def test_genuine_trades_with_combined_flags_and_provenance():
    """
    Comprova a preservação de negócios com flags combinadas (ex: BID + LAST + BUY),
    classificação precisa da agressão e distinção estrita entre histórico e tempo real.
    """
    reconciler = TickReconciler()
    boot_msc = 1792411500000  # Momento em que o coletor iniciou

    raw_trades = [
        # 1. Negócio de Compra com flags combinadas ocorrido ANTES do boot
        {
            "time": 1792411400, "time_msc": 1792411400000,
            "last": 125050.0, "volume_real": 5.0,
            "flags": TICK_FLAG_BID | TICK_FLAG_LAST | TICK_FLAG_BUY
        },
        # 2. Negócio de Venda com flags combinadas ocorrido ANTES do boot
        {
            "time": 1792411450, "time_msc": 1792411450000,
            "last": 125040.0, "volume_real": 2.0,
            "flags": TICK_FLAG_ASK | TICK_FLAG_LAST | TICK_FLAG_SELL
        },
        # 3. Negócio direto de leilão/cruzamento sem flag BUY/SELL
        {
            "time": 1792411480, "time_msc": 1792411480000,
            "last": 125045.0, "volume_real": 100.0,
            "flags": TICK_FLAG_LAST | TICK_FLAG_VOLUME
        },
        # 4. Negócio executado APÓS o boot durante o streaming
        {
            "time": 1792411600, "time_msc": 1792411600000,
            "last": 125060.0, "volume_real": 10.0,
            "flags": TICK_FLAG_LAST | TICK_FLAG_BUY
        }
    ]

    result = reconciler.reconcile_raw_ticks(
        raw_trades,
        symbol="WINZ26",
        provenance="REALTIME_STREAM",
        cutoff_realtime_msc=boot_msc
    )

    assert len(result) == 4

    # Validação do Negócio 1 (Compra anterior ao boot -> RECOVERED_HISTORICAL_MT5)
    t1 = result[0]
    assert t1.trade_side == "BUY"
    assert t1.price == 125050.0
    assert t1.volume_real == 5.0
    assert t1.provenance == "RECOVERED_HISTORICAL_MT5"

    # Validação do Negócio 2 (Venda anterior ao boot -> RECOVERED_HISTORICAL_MT5)
    t2 = result[1]
    assert t2.trade_side == "SELL"
    assert t2.price == 125040.0
    assert t2.volume_real == 2.0
    assert t2.provenance == "RECOVERED_HISTORICAL_MT5"

    # Validação do Negócio 3 (Sem BUY/SELL -> CROSS, não infere agressão)
    t3 = result[2]
    assert t3.trade_side == "CROSS"
    assert t3.provenance == "RECOVERED_HISTORICAL_MT5"

    # Validação do Negócio 4 (Após o boot -> REALTIME_STREAM)
    t4 = result[3]
    assert t4.trade_side == "BUY"
    assert t4.provenance == "REALTIME_STREAM"

def test_checkpoint_corruption_fails_safe(tmp_path):
    """
    Comprova que o engine executa parada segura com erro explícito diante
    de checkpoints corrompidos ou inconsistentes (ex: replicado > capturado).
    """
    db_file = tmp_path / "corrupt_spool.db"
    spool = DurableSpool(db_file)
    config = load_config()

    # Injeta intencionalmente inconsistência de checkpoint: replicado > capturado
    with spool._get_connection() as conn:
        conn.execute("""
        INSERT INTO checkpoints_two_stage (stream_key, last_captured_msc, last_replicated_msc, updated_at_utc)
        VALUES ('WINZ26', 1000, 2000, '2026-10-19T12:00:00+00:00');
        """)
        conn.commit()

    mock_reader = MagicMock()
    mock_reader.initialize.return_value = True
    mock_reader.is_connected.return_value = True
    mock_reader.select_symbol.return_value = True

    engine = MarketCollectorEngine(config=config, reader=mock_reader, spool=spool, writer=MagicMock())

    with pytest.raises(RuntimeError) as exc_info:
        engine.initialize_and_recover()

    assert "Inconsistência crítica: last_replicated_msc (2000) > last_captured_msc (1000)" in str(exc_info.value)
    assert engine._is_running is False
    mock_reader.shutdown.assert_called_once()

def test_controlled_reconnection_on_mt5_transient_failure(tmp_path):
    """
    Comprova que desconexões transitórias do MT5 durante a captura acionam a rotina
    de reconexão com backoff e retomam a coleta sem abortar o processo.
    """
    db_file = tmp_path / "reconnect_spool.db"
    spool = DurableSpool(db_file)
    config = load_config()

    mock_reader = MagicMock()
    # Inicialmente desconectado, reconecta na segunda tentativa
    mock_reader.is_connected.side_effect = [False, False, True, True]
    mock_reader.initialize.return_value = True
    mock_reader.select_symbol.return_value = True
    mock_reader.fetch_ticks_from.return_value = [
        {"time": 1792411600, "time_msc": 1792411600000, "last": 125100.0, "volume_real": 1.0, "flags": TICK_FLAG_LAST | TICK_FLAG_BUY}
    ]

    engine = MarketCollectorEngine(config=config, reader=mock_reader, spool=spool, writer=MagicMock())
    engine.boot_time_msc = 1792411000000

    with patch("time.sleep"):  # Evita atrasos nos testes unitários
        inserted = engine.capture_step()

    assert inserted == 1
    assert mock_reader.initialize.call_count >= 1
