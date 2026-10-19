from datetime import datetime, timezone, timedelta
from src.domain.models import TradeTick
from src.buffer.durable_spool import DurableSpool
from src.storage.postgres_writer import PostgresMarketWriter
from src.config import load_config
from src.engine import MarketCollectorEngine
from unittest.mock import MagicMock

def test_postgres_failover_and_recovery(tmp_path, test_postgres_writer):
    """
    Comprova que uma queda do PostgreSQL não interrompe a captura local
    e que todos os dados são drenados para o PostgreSQL após o restabelecimento.
    """
    db_file = tmp_path / "failover_spool.db"
    spool = DurableSpool(db_file)
    config = load_config()

    # 1. Simular PostgreSQL temporariamente indisponível (porta inválida 5499)
    broken_writer = PostgresMarketWriter(host="127.0.0.1", port=5499, database="market_data_test", user="postgres", password="password")
    
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    engine = MarketCollectorEngine(config=config, reader=mock_reader, spool=spool, writer=broken_writer)

    t_utc = datetime(2026, 10, 19, 15, 0, 0, tzinfo=timezone.utc)
    raw_ticks = [
        {"time": 1791882200, "time_msc": 1791882200000 + i * 20, "last": 125200.0, "volume_real": 1.0, "flags": 32}
        for i in range(15)
    ]

    # Captura local no SQLite WAL continua sem erro
    inserted = engine.capture_step(raw_ticks=raw_ticks)
    assert inserted == 15
    ckp1 = spool.get_checkpoints("WINZ26")
    assert ckp1.last_captured_msc == 1791882200280
    assert ckp1.last_replicated_msc == 0

    # Tentativa de replicação falha graciosamente sem perder dados
    replicated_failed = engine.replicate_pending_to_postgres()
    assert replicated_failed == 0
    assert len(spool.get_pending_ticks()) == 15

    # 2. Restabelecimento da conexão com o banco isolado de teste (market_data_test)
    real_writer = test_postgres_writer
    engine.writer = real_writer

    try:
        # Dreno automático pós-restabelecimento
        replicated_success = engine.replicate_pending_to_postgres()
        assert replicated_success == 15

        ckp2 = spool.get_checkpoints("WINZ26")
        assert ckp2.last_captured_msc == 1791882200280
        assert ckp2.last_replicated_msc == 1791882200280
        assert len(spool.get_pending_ticks()) == 0
    finally:
        with test_postgres_writer.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM market_ticks_raw WHERE time_msc >= 1791882200000 AND time_msc <= 1791882200280;")
            conn.commit()
