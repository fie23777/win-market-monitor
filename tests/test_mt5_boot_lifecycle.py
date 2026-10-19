import pytest
from unittest.mock import MagicMock, patch
from src.config import load_config
from src.engine import MarketCollectorEngine
from src.buffer.durable_spool import DurableSpool
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.storage.postgres_writer import PostgresMarketWriter
from src.main import CollectorApp

def test_engine_initialization_success(tmp_path):
    """
    Comprova que o boot inicializa o MT5 com sucesso, verifica conectividade,
    seleciona o símbolo configurado e conclui o recovery.
    """
    db_file = tmp_path / "spool_test.db"
    spool = DurableSpool(db_file)
    config = load_config()

    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_reader.initialize.return_value = True
    mock_reader.is_connected.return_value = True
    mock_reader.select_symbol.return_value = True

    mock_writer = MagicMock(spec=PostgresMarketWriter)

    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        writer=mock_writer
    )

    res = engine.initialize_and_recover()
    assert res["status"] in ("INITIALIZED", "RECOVERED_ON_STARTUP")
    assert engine._is_running is True

    mock_reader.initialize.assert_called_once()
    mock_reader.is_connected.assert_called_once()
    mock_reader.select_symbol.assert_called_once_with(config.symbol)

def test_engine_initialization_failure_raises_explicit_error(tmp_path):
    """
    Comprova que se reader.initialize() falhar (ex: terminal MT5 fechado),
    o engine levanta RuntimeError explícito, chama shutdown e NÃO inicia o loop.
    """
    db_file = tmp_path / "spool_test.db"
    spool = DurableSpool(db_file)
    config = load_config()

    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_reader.initialize.return_value = False

    mock_writer = MagicMock(spec=PostgresMarketWriter)

    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        writer=mock_writer
    )

    with pytest.raises(RuntimeError) as exc_info:
        engine.initialize_and_recover()

    assert "Falha crítica na inicialização do MT5" in str(exc_info.value)
    assert engine._is_running is False
    mock_reader.shutdown.assert_called_once()

def test_engine_terminal_disconnected_raises_explicit_error(tmp_path):
    """
    Comprova que se o terminal MT5 estiver aberto mas desconectado da corretora,
    o engine falha explicitamente com RuntimeError e chama shutdown.
    """
    db_file = tmp_path / "spool_test.db"
    spool = DurableSpool(db_file)
    config = load_config()

    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_reader.initialize.return_value = True
    mock_reader.is_connected.return_value = False

    mock_writer = MagicMock(spec=PostgresMarketWriter)

    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        writer=mock_writer
    )

    with pytest.raises(RuntimeError) as exc_info:
        engine.initialize_and_recover()

    assert "Terminal MT5 inicializado, porém desconectado do servidor da corretora" in str(exc_info.value)
    assert engine._is_running is False
    mock_reader.shutdown.assert_called_once()

def test_engine_symbol_selection_failure_raises_explicit_error(tmp_path):
    """
    Comprova que se o símbolo não puder ser selecionado no Market Watch,
    o engine falha explicitamente e executa shutdown seguro.
    """
    db_file = tmp_path / "spool_test.db"
    spool = DurableSpool(db_file)
    config = load_config()

    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_reader.initialize.return_value = True
    mock_reader.is_connected.return_value = True
    mock_reader.select_symbol.return_value = False

    mock_writer = MagicMock(spec=PostgresMarketWriter)

    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        writer=mock_writer
    )

    with pytest.raises(RuntimeError) as exc_info:
        engine.initialize_and_recover()

    assert f"Símbolo {config.symbol} não pôde ser selecionado" in str(exc_info.value)
    assert engine._is_running is False
    mock_reader.shutdown.assert_called_once()

def test_engine_shutdown_releases_resources(tmp_path):
    """
    Comprova que shutdown() do engine invoca shutdown() do MT5 com segurança.
    """
    db_file = tmp_path / "spool_test.db"
    spool = DurableSpool(db_file)
    config = load_config()

    mock_reader = MagicMock(spec=MT5ReadOnlyReader)
    mock_writer = MagicMock(spec=PostgresMarketWriter)

    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        writer=mock_writer
    )

    engine.shutdown()
    mock_reader.shutdown.assert_called_once()
    assert engine._is_running is False

def test_collector_app_exits_on_initialization_failure():
    """
    Comprova que CollectorApp.run encerra o processo via sys.exit(1)
    se initialize_and_recover falhar, chamando shutdown e sem entrar no loop.
    """
    app = CollectorApp()
    app.engine = MagicMock()
    app.engine.initialize_and_recover.side_effect = RuntimeError("MT5 offline")

    with pytest.raises(SystemExit) as exc_info:
        app.run()

    assert exc_info.value.code == 1
    app.engine.shutdown.assert_called_once()
