from datetime import datetime, timezone, timedelta
from src.domain.models import TradeTick
from src.buffer.durable_spool import DurableSpool
from src.storage.postgres_writer import PostgresMarketWriter
from src.config import load_config
from src.engine import MarketCollectorEngine
from unittest.mock import MagicMock

def test_startup_preserves_and_drains_existing_spool_records(tmp_path):
    """
    Comprova que na inicialização o coletor NUNCA apaga o spool local existente,
    mas sim identifica registros pendentes do encerramento anterior e os replica.
    """
    db_file = tmp_path / "persistent_spool.db"
    initial_spool = DurableSpool(db_file)
    t_utc = datetime(2026, 10, 19, 15, 30, 0, tzinfo=timezone.utc)

    # 1. Deixar 8 ticks pendentes no spool
    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791882300000 + i * 10,
            time_utc=t_utc + timedelta(milliseconds=i * 10),
            price=125300.0,
            volume_real=1.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint=f"fp_startup_persist_{i}",
            provenance="REALTIME_STREAM"
        )
        for i in range(8)
    ]
    initial_spool.append_ticks_batch(ticks, stream_key="WINZ26")
    del initial_spool  # Processo encerra abruptamente

    # 2. Novo processo do coletor inicializa sobre o mesmo arquivo de spool
    config = load_config()
    reopened_spool = DurableSpool(db_file)
    
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    real_writer = PostgresMarketWriter(host="127.0.0.1", port=5434, database="market_data", user="postgres", password="password")
    engine = MarketCollectorEngine(config=config, reader=mock_reader, spool=reopened_spool, writer=real_writer)

    # O método de inicialização deve drenar os 8 registros pendentes
    res = engine.initialize_and_recover()
    assert res["drained_pending_count"] == 8
    assert len(reopened_spool.get_pending_ticks()) == 0

    # Validar no PostgreSQL 17 real
    with real_writer.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM market_ticks_raw WHERE tick_fingerprint LIKE 'fp_startup_persist_%';")
            count = cur.fetchone()[0]
            assert count == 8
