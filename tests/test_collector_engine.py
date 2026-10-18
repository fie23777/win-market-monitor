from unittest.mock import MagicMock
from src.config import load_config
from src.engine import MarketCollectorEngine
from src.buffer.durable_spool import DurableSpool
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.storage.postgres_writer import PostgresMarketWriter

def test_collector_engine_lifecycle_and_two_stage_checkpoints(tmp_path):
    """
    Comprova o ciclo de vida ponta a ponta do MarketCollectorEngine:
    1. Captura durável no buffer local (Estágio 1).
    2. Replicação com avanço de checkpoint remoto (Estágio 2).
    3. Inicialização e dreno pós-reconexão.
    """
    db_file = tmp_path / "engine_spool.db"
    spool = DurableSpool(db_file)
    
    config = load_config()
    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_reader.is_connected.return_value = True

    mock_writer = MagicMock(spec=PostgresMarketWriter)
    mock_writer.write_ticks_batch.return_value = 5

    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        writer=mock_writer
    )

    # 1. Simular captura de 5 ticks brutos
    raw_ticks = [
        {"time": 1791882000, "time_msc": 1791882000100 + i * 10, "last": 125000.0, "volume_real": 2.0, "flags": 32}
        for i in range(5)
    ]

    inserted = engine.capture_step(raw_ticks=raw_ticks)
    assert inserted == 5

    ckp1 = spool.get_checkpoints("WINZ26")
    assert ckp1.last_captured_msc == 1791882000140
    assert ckp1.last_replicated_msc == 0

    # 2. Replicar para o PostgreSQL
    replicated = engine.replicate_pending_to_postgres()
    assert replicated == 5
    mock_writer.write_ticks_batch.assert_called_once()

    ckp2 = spool.get_checkpoints("WINZ26")
    assert ckp2.last_captured_msc == 1791882000140
    assert ckp2.last_replicated_msc == 1791882000140

    # 3. Segunda iteração com mesmos dados (sobreposição) -> 0 inserções novas
    inserted_again = engine.capture_step(raw_ticks=raw_ticks)
    assert inserted_again == 0
