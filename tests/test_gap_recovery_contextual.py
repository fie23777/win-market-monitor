from unittest.mock import MagicMock
from src.recovery.gap_recovery import GapRecoveryManager
from src.reconciliation.tick_reconciler import TickReconciler
from src.connection.mt5_reader import MT5ReadOnlyReader

def test_short_inactivity_during_trading_hours_is_not_treated_as_data_loss():
    """Comprova que um lapso de 4 segundos durante o pregão ativo não gera falso positivo de perda."""
    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_reader.is_connected.return_value = True
    
    reconciler = TickReconciler()
    manager = GapRecoveryManager(reader=mock_reader, reconciler=reconciler, inactivity_threshold_seconds=15)

    last_seen = 1791882000000
    curr_time = 1791882004000  # 4 segundos depois

    res = manager.evaluate_and_recover(
        symbol="WINZ26",
        last_seen_msc=last_seen,
        current_time_msc=curr_time,
        is_connected=True
    )

    assert res["status"] == "NATURAL_MARKET_INACTIVITY"
    assert len(res["recovered_ticks"]) == 0
    mock_reader.fetch_ticks_range.assert_not_called()

def test_recovery_from_historical_mt5_after_gap():
    """Comprova a recuperação e etiquetagem quando o MT5 retorna o histórico do período perdido."""
    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_reader.is_connected.return_value = True
    mock_reader.fetch_ticks_range.return_value = [
        {"time": 1791882020, "time_msc": 1791882020000, "last": 125030.0, "volume_real": 3.0, "flags": 32},
        {"time": 1791882025, "time_msc": 1791882025000, "last": 125035.0, "volume_real": 2.0, "flags": 64},
    ]

    reconciler = TickReconciler()
    manager = GapRecoveryManager(reader=mock_reader, reconciler=reconciler, inactivity_threshold_seconds=15)

    res = manager.evaluate_and_recover(
        symbol="WINZ26",
        last_seen_msc=1791882000000,
        current_time_msc=1791882030000,  # 30 segundos depois
        is_connected=True
    )

    assert res["status"] == "RECOVERED"
    assert len(res["recovered_ticks"]) == 2
    assert all(t.provenance == "RECOVERED_HISTORICAL_MT5" for t in res["recovered_ticks"])
    assert res["quality_event"].provenance_uncertainty is False

def test_unrecovered_gap_when_broker_returns_no_history_after_disconnection():
    """Comprova o registro formal de incerteza se a corretora não entregar o histórico pós-desconexão."""
    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_reader.is_connected.return_value = False
    mock_reader.fetch_ticks_range.return_value = []

    reconciler = TickReconciler()
    manager = GapRecoveryManager(reader=mock_reader, reconciler=reconciler, inactivity_threshold_seconds=15)

    res = manager.evaluate_and_recover(
        symbol="WINZ26",
        last_seen_msc=1791882000000,
        current_time_msc=1791882040000,
        is_connected=False
    )

    assert res["status"] == "UNRECOVERED_GAP"
    assert res["quality_event"].provenance_uncertainty is True
    assert res["quality_event"].event_type == "DISCONNECTION_DATA_UNCERTAIN"
