import pytest
from datetime import datetime, timezone, timedelta
from src.domain.models import M2CandleClosed
from src.buffer.durable_spool import DurableSpool
from src.storage.postgres_writer import PostgresMarketWriter

def test_m2_closed_candle_persistence_and_validation(tmp_path, test_postgres_writer):
    """
    Comprova a regra estrita de que apenas candles M2 completamente fechados
    são validados e persistidos no SQLite WAL e no PostgreSQL 17 de testes (market_data_test).
    """
    db_file = tmp_path / "candle_test.db"
    spool = DurableSpool(db_file)
    writer = test_postgres_writer

    open_utc = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
    close_utc = datetime(2026, 10, 19, 12, 2, 0, tzinfo=timezone.utc)
    received_utc = datetime(2026, 10, 19, 12, 2, 0, 150000, tzinfo=timezone.utc)

    # 1. Candle M2 consolidado válido
    candle = M2CandleClosed(
        symbol="WINZ26",
        open_time_utc=open_utc,
        close_time_utc=close_utc,
        open=125000.0,
        high=125050.0,
        low=124980.0,
        close=125020.0,
        volume_real=1500.0,
        tick_volume=120,
        spread=1,
        is_closed=True,
        provenance="REALTIME_STREAM",
        received_at_utc=received_utc
    )

    # Persistência no buffer local durável
    assert spool.append_candle_m2(candle) is True

    try:
        # Persistência no PostgreSQL 17 real
        assert writer.write_candle_m2(candle) is True

        # Validar no banco real
        with writer.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT open, high, low, close, is_closed FROM candles_m2 WHERE symbol = 'WINZ26' AND open_time_utc = %s;", (open_utc,))
                row = cur.fetchone()
                assert row is not None
                assert row[0] == 125000.0
                assert row[3] == 125020.0
                assert row[4] is True
    finally:
        with writer.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM candles_m2 WHERE symbol = 'WINZ26' AND open_time_utc = %s;", (open_utc,))
            conn.commit()

def test_invalid_candle_prices_are_rejected():
    """Valida que preços incoerentes (ex: low > high) são rejeitados com ValueError."""
    open_utc = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
    close_utc = datetime(2026, 10, 19, 12, 2, 0, tzinfo=timezone.utc)

    invalid_candle = M2CandleClosed(
        symbol="WINZ26",
        open_time_utc=open_utc,
        close_time_utc=close_utc,
        open=125000.0,
        high=124900.0,
        low=125050.0,  # Low maior que High!
        close=125000.0,
        volume_real=100.0,
        tick_volume=50,
        spread=1
    )

    with pytest.raises(ValueError, match="Low .* maior que High"):
        invalid_candle.validate()
