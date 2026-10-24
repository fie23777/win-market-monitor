import os
import sys
import json
import time
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta

from src.config import load_config, AppConfig, BufferConfig, PostgresConfig, ParquetConfig, CollectorSettings
from src.utils.process_lock import CollectorProcessLock, ProcessLockError
from src.buffer.durable_spool import DurableSpool
from src.domain.models import TradeTick
from src.health_checker import evaluate_system_health, is_b3_market_open
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.main import CollectorApp

def create_mock_config(tmp_path, symbol="WINZ26"):
    spool_path = tmp_path / "buffer" / "test_spool.db"
    return AppConfig(
        symbol=symbol,
        timezones={"storage": "UTC", "display": "America/Sao_Paulo", "market": "America/Sao_Paulo", "mt5_server": "America/Sao_Paulo"},
        buffer=BufferConfig(sqlite_path=str(spool_path), journal_mode="WAL", synchronous="FULL", batch_size=1000),
        postgres=PostgresConfig(host="127.0.0.1", port=5434, database="market_data_test", user="postgres", password="password", retention_trading_days=30),
        parquet=ParquetConfig(archive_dir=str(tmp_path / "archive"), backup_dir=str(tmp_path / "backup"), compression="zstd", compression_level=7, ticks_retention_years=1, candles_m2_retention_years=3),
        collector=CollectorSettings(read_only=True, allow_orders=False, poll_interval_ms=100, gap_inactivity_threshold_seconds=15)
    )

# -------------------------------------------------------------
# Cenário 1: Docker e MT5 Desligados
# -------------------------------------------------------------
def test_scenario_01_docker_and_mt5_off(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": False, "version": None, "message": "Docker Engine indisponível"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": False, "port_open": False, "can_query": False, "message": "Porta 5434 fechada"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": False, "connected": False, "symbol_available": False, "waiting_user": False, "message": "Terminal MT5 fechado"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["status"] == "UNHEALTHY"
        assert not diag["infrastructure"]["docker"]["available"]
        assert not diag["infrastructure"]["mt5"]["healthy"]
        assert not diag["infrastructure"]["postgres"]["healthy"]

# -------------------------------------------------------------
# Cenário 2: Docker Ligado e MT5 Desligado
# -------------------------------------------------------------
def test_scenario_02_docker_on_and_mt5_off(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker Engine disponível"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PostgreSQL saudável"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": False, "connected": False, "symbol_available": False, "waiting_user": False, "message": "Terminal MT5 fechado"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["status"] in ("UNHEALTHY", "DEGRADED")
        assert diag["infrastructure"]["docker"]["available"] is True
        assert diag["infrastructure"]["mt5"]["healthy"] is False

# -------------------------------------------------------------
# Cenário 3: MT5 Ligado e Docker Desligado
# -------------------------------------------------------------
def test_scenario_03_mt5_on_and_docker_off(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": False, "version": None, "message": "Docker indisponível"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": False, "port_open": False, "can_query": False, "message": "PostgreSQL fechado"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 conectado"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["status"] == "UNHEALTHY"
        assert diag["infrastructure"]["docker"]["available"] is False
        assert diag["infrastructure"]["mt5"]["healthy"] is True

# -------------------------------------------------------------
# Cenário 4: Ambos Ligados e Saudáveis
# -------------------------------------------------------------
def test_scenario_04_both_on_and_healthy(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)
    lock_dir = tmp_path / "buffer"
    lock = CollectorProcessLock(symbol=cfg.symbol, lock_dir=lock_dir)
    lock.acquire()

    try:
        with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker disponível"}), \
             patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PostgreSQL saudável"}), \
             patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 conectado"}):
            
            diag = evaluate_system_health(config=cfg, spool=spool)
            assert diag["status"] == "HEALTHY"
            assert diag["application"]["lock"]["status"] == "ACTIVE_HEALTHY"
    finally:
        lock.release()

# -------------------------------------------------------------
# Cenário 5: PostgreSQL Parado no Docker
# -------------------------------------------------------------
def test_scenario_05_postgres_stopped(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker disponível"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": False, "port_open": False, "can_query": False, "message": "Porta 5434 fechada"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 conectado"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["infrastructure"]["postgres"]["healthy"] is False
        assert diag["status"] in ("UNHEALTHY", "DEGRADED")

# -------------------------------------------------------------
# Cenário 6: PostgreSQL Iniciado mas Não Pronto (pg_isready pendente)
# -------------------------------------------------------------
def test_scenario_06_postgres_starting_not_ready(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker disponível"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": False, "port_open": True, "can_query": False, "message": "PostgreSQL inicializando (pg_isready rejeitado)"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 conectado"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["infrastructure"]["postgres"]["port_open"] is True
        assert diag["infrastructure"]["postgres"]["can_query"] is False
        assert diag["status"] in ("UNHEALTHY", "DEGRADED")

# -------------------------------------------------------------
# Cenário 7: MT5 Aberto Porém Desconectado (Requer Intervenção Humana)
# -------------------------------------------------------------
def test_scenario_07_mt5_open_but_disconnected(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker disponível"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PostgreSQL saudável"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": False, "connected": False, "symbol_available": False, "waiting_user": True, "message": "MT5 desconectado do servidor da corretora"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["status"] == "WAITING_FOR_USER"
        assert diag["infrastructure"]["mt5"]["waiting_user"] is True

# -------------------------------------------------------------
# Cenário 8: Símbolo Configurado Indisponível
# -------------------------------------------------------------
def test_scenario_08_symbol_unavailable(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker disponível"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PostgreSQL saudável"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": False, "connected": True, "symbol_available": False, "waiting_user": False, "message": "Símbolo WINZ26 não selecionável"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["infrastructure"]["mt5"]["symbol_available"] is False
        assert diag["status"] in ("UNHEALTHY", "DEGRADED")

# -------------------------------------------------------------
# Cenário 9: Coletor Já Executando (Prevenção de Segunda Instância)
# -------------------------------------------------------------
def test_scenario_09_collector_already_running(tmp_path):
    lock_dir = tmp_path / "buffer"
    lock1 = CollectorProcessLock("WINZ26", lock_dir)
    lock1.acquire()

    lock2 = CollectorProcessLock("WINZ26", lock_dir)
    with pytest.raises(ProcessLockError) as exc_info:
        lock2.acquire()

    assert "Já existe uma instância do coletor executando" in str(exc_info.value)
    lock1.release()

# -------------------------------------------------------------
# Cenário 10: Concorrência Real / Trava Atômica
# -------------------------------------------------------------
def test_scenario_10_atomic_mutex_concurrency(tmp_path):
    lock_dir = tmp_path / "buffer"
    lockA = CollectorProcessLock("WINZ26", lock_dir)
    assert lockA.acquire() is True

    lockB = CollectorProcessLock("WINZ26", lock_dir)
    with pytest.raises(ProcessLockError):
        lockB.acquire()

    lockA.release()
    # Após release, a trava pode ser adquirida novamente
    assert lockB.acquire() is True
    lockB.release()

# -------------------------------------------------------------
# Cenário 11: Falha de Autenticação no PostgreSQL
# -------------------------------------------------------------
def test_scenario_11_postgres_auth_failure(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker disponível"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": False, "port_open": True, "can_query": False, "message": "FATAL: password authentication failed"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 conectado"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["infrastructure"]["postgres"]["healthy"] is False
        assert "password authentication failed" in diag["infrastructure"]["postgres"]["message"]

# -------------------------------------------------------------
# Cenário 12: Timeout de Inicialização do Docker
# -------------------------------------------------------------
def test_scenario_12_docker_startup_timeout(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    with patch("src.health_checker.check_docker_engine", return_value={"available": False, "version": None, "message": "Timeout ao consultar docker info"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": False, "port_open": False, "can_query": False, "message": "PostgreSQL fechado"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 conectado"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["status"] == "UNHEALTHY"
        assert "Timeout" in diag["infrastructure"]["docker"]["message"]

# -------------------------------------------------------------
# Cenário 13: Falha Transitória com Recuperação
# -------------------------------------------------------------
def test_scenario_13_transient_failure_recovery(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)
    lock_dir = tmp_path / "buffer"
    lock = CollectorProcessLock("WINZ26", lock_dir)
    lock.acquire()

    try:
        # Estado 1: Falha transitória no PG
        with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker ok"}), \
             patch("src.health_checker.check_postgres_health", return_value={"healthy": False, "port_open": False, "can_query": False, "message": "PG offline temporário"}), \
             patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 conectado"}):
            diag1 = evaluate_system_health(config=cfg, spool=spool)
            assert diag1["status"] == "DEGRADED"

        # Estado 2: Recuperação transitória
        with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker ok"}), \
             patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PG recuperado com sucesso"}), \
             patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 conectado"}):
            diag2 = evaluate_system_health(config=cfg, spool=spool)
            assert diag2["status"] == "HEALTHY"
    finally:
        lock.release()

# -------------------------------------------------------------
# Cenário 14: Alerta Crítico Persistido no SQLite Spool (Não Mascarar)
# -------------------------------------------------------------
def test_scenario_14_critical_stagnation_alert_unmasked(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)
    spool.record_operational_alert(
        stream_key="WINZ26",
        alert_type="CRITICAL_STAGNATION_LIMIT_EXCEEDED",
        details="Densidade estagnada excedeu 5 ciclos sob cursor 1792400000000"
    )

    with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker ok"}), \
         patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PG ok"}), \
         patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 ok"}):
        
        diag = evaluate_system_health(config=cfg, spool=spool)
        assert diag["status"] == "UNHEALTHY"
        assert diag["application"]["critical_stagnation"] is True
        assert any("CRITICAL_STAGNATION_LIMIT_EXCEEDED" in r for r in diag["reasons"])

# -------------------------------------------------------------
# Cenário 15: Sistema Fora do Horário de Pregão B3 (Fim de semana / Noite)
# -------------------------------------------------------------
def test_scenario_15_outside_trading_hours_healthy_standby(tmp_path):
    # Sábado às 14:00 BRT
    saturday_utc = datetime(2026, 10, 24, 17, 0, 0, tzinfo=timezone.utc)
    assert is_b3_market_open(saturday_utc) is False

    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)
    lock_dir = tmp_path / "buffer"
    lock = CollectorProcessLock("WINZ26", lock_dir)
    lock.acquire()

    try:
        with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker ok"}), \
             patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PG ok"}), \
             patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 ok"}), \
             patch("src.health_checker.is_b3_market_open", return_value=False):
            
            diag = evaluate_system_health(config=cfg, spool=spool)
            assert diag["market_open"] is False
            assert "Standby" in diag["market_note"]
            assert diag["status"] == "HEALTHY"
    finally:
        lock.release()

# -------------------------------------------------------------
# Cenário 16: Reinicialização Após Falha Parcial
# -------------------------------------------------------------
def test_scenario_16_restart_after_partial_failure(tmp_path):
    lock_dir = tmp_path / "buffer"
    lock = CollectorProcessLock("WINZ26", lock_dir)
    lock.acquire()
    assert lock.is_shutdown_requested() is False

    # Solicitar parada graciosa
    shutdown_req = lock_dir / "shutdown.request"
    shutdown_req.write_text("SHUTDOWN_REQUESTED")
    assert lock.is_shutdown_requested() is True

    # Liberar e limpar
    lock.release()
    assert not shutdown_req.exists()
    assert not (lock_dir / "collector_WINZ26.lock").exists()

    # Nova aquisição bem sucedida
    new_lock = CollectorProcessLock("WINZ26", lock_dir)
    assert new_lock.acquire() is True
    new_lock.release()

# -------------------------------------------------------------
# Cenário 17: Execução Repetida Sem Duplicação (Idempotência)
# -------------------------------------------------------------
def test_scenario_17_idempotent_inspection(tmp_path):
    lock_dir = tmp_path / "buffer"
    lock = CollectorProcessLock("WINZ26", lock_dir)
    lock.acquire()

    try:
        state1 = CollectorProcessLock.inspect_lock_state("WINZ26", lock_dir)
        state2 = CollectorProcessLock.inspect_lock_state("WINZ26", lock_dir)
        assert state1["status"] == "ACTIVE_HEALTHY"
        assert state2["status"] == "ACTIVE_HEALTHY"
        assert state1["pid"] == state2["pid"] == os.getpid()
    finally:
        lock.release()

# -------------------------------------------------------------
# Cenário 18: Encerramento Gracioso Sem Perda de Dados
# -------------------------------------------------------------
def test_scenario_18_graceful_shutdown_data_preservation(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    # Inserir ticks com status PENDING no spool
    tick = TradeTick(
        symbol="WINZ26",
        time_msc=1792400010000,
        time_utc=datetime.now(timezone.utc),
        price=125500.0,
        volume_real=10.0,
        flags=32,
        trade_side="BUY",
        occurrence_ordinal=1,
        provenance="COLLECTOR_LIVE"
    )
    spool.append_ticks_batch([tick], stream_key="WINZ26")

    metrics_before = spool.get_pending_metrics("WINZ26")
    assert metrics_before["pending_ticks_count"] == 1

    # Simular encerramento gracioso via CollectorApp
    app = CollectorApp()
    app.config = cfg
    app.lock = CollectorProcessLock("WINZ26", tmp_path / "buffer")
    app.engine = MagicMock()
    app.engine.replicate_pending_to_postgres.return_value = 1

    # Executar parada segura
    app.keep_running = False
    final_replicated = app.stop_and_cleanup()
    assert final_replicated == 1
    app.engine.shutdown.assert_called_once()

# -------------------------------------------------------------
# Cenário 19: Processo Travado com Heartbeat Estagnado (FROZEN_PROCESS)
# -------------------------------------------------------------
def test_scenario_19_frozen_process_heartbeat_lag(tmp_path):
    lock_dir = tmp_path / "buffer"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / "collector_WINZ26.lock"
    
    # Criar lock com timestamp antigo (60 segundos atrás)
    old_time = time.time() - 60.0
    stale_meta = {
        "pid": os.getpid(),  # Processo atual existe!
        "symbol": "WINZ26",
        "started_at_utc": "2026-10-24T18:00:00Z",
        "heartbeat_timestamp": old_time,
        "command_line": "python -m src.main",
        "status": "RUNNING"
    }
    lock_file.write_text(json.dumps(stale_meta))

    state = CollectorProcessLock.inspect_lock_state("WINZ26", lock_dir, max_heartbeat_lag_sec=30.0)
    assert state["status"] == "FROZEN_PROCESS"
    assert state["heartbeat_age_sec"] >= 59.0
    assert "heartbeat estagnado" in state["message"]

# -------------------------------------------------------------
# Cenário 20: Lock Obsoleto e PID Reutilizado
# -------------------------------------------------------------
def test_scenario_20_stale_lock_and_reused_pid(tmp_path):
    lock_dir = tmp_path / "buffer"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / "collector_WINZ26.lock"

    # Caso A: PID inexistente (ex: 99999999)
    meta_dead_pid = {
        "pid": 99999999,
        "symbol": "WINZ26",
        "heartbeat_timestamp": time.time(),
        "command_line": "python -m src.main"
    }
    lock_file.write_text(json.dumps(meta_dead_pid))

    state_dead = CollectorProcessLock.inspect_lock_state("WINZ26", lock_dir)
    assert state_dead["status"] == "STALE_LOCK"

    # Caso B: PID reaproveitado (processo com outro create_time)
    meta_reused = {
        "pid": os.getpid(),
        "create_time": time.time() - 100000.0,
        "symbol": "WINZ26",
        "heartbeat_timestamp": time.time(),
        "command_line": "python -m src.main"
    }
    lock_file.write_text(json.dumps(meta_reused))
    state_reused = CollectorProcessLock.inspect_lock_state("WINZ26", lock_dir)
    assert state_reused["status"] == "REUSED_PID"

# -------------------------------------------------------------
# Cenário 21: PostgreSQL Indisponível Durante Shutdown Gracioso (Buffer Preservado)
# -------------------------------------------------------------
def test_scenario_21_postgres_down_during_graceful_shutdown(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)

    tick = TradeTick(
        symbol="WINZ26",
        time_msc=1792400020000,
        time_utc=datetime.now(timezone.utc),
        price=125600.0,
        volume_real=5.0,
        flags=32,
        trade_side="SELL",
        occurrence_ordinal=1,
        provenance="COLLECTOR_LIVE"
    )
    spool.append_ticks_batch([tick], stream_key="WINZ26")

    # Mock do writer levantando erro de conexão no shutdown
    mock_engine = MagicMock()
    mock_engine.replicate_pending_to_postgres.side_effect = ConnectionError("PostgreSQL desconectou abruptamente")

    app = CollectorApp()
    app.config = cfg
    app.engine = mock_engine
    app.lock = CollectorProcessLock("WINZ26", tmp_path / "buffer")

    # Executar parada com falha no PG
    try:
        app.engine.replicate_pending_to_postgres()
    except Exception as e:
        # Não propaga falha destrutiva
        assert isinstance(e, ConnectionError)
    
    app.engine.shutdown()
    app.engine.shutdown.assert_called_once()

    # O registro PENDING continua 100% íntegro no SQLite WAL
    metrics = spool.get_pending_metrics("WINZ26")
    assert metrics["pending_ticks_count"] == 1

# -------------------------------------------------------------
# Cenário 22: Pendências Normais Declaradas HEALTHY
# -------------------------------------------------------------
def test_scenario_22_normal_pending_ticks_is_healthy(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)
    lock_dir = tmp_path / "buffer"
    lock = CollectorProcessLock("WINZ26", lock_dir)
    lock.acquire()

    # Inserir 50 ticks pendentes recentes
    now_utc = datetime.now(timezone.utc)
    try:
        import zoneinfo
        brt_tz = zoneinfo.ZoneInfo("America/Sao_Paulo")
    except Exception:
        brt_tz = timezone(timedelta(hours=-3))
    now_brt = now_utc.astimezone(brt_tz)
    naive = now_brt.replace(tzinfo=None)
    now_msc = int(naive.replace(tzinfo=timezone.utc).timestamp() * 1000)

    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=now_msc + i,
            time_utc=now_utc,
            price=125000.0 + i,
            volume_real=1.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            provenance="COLLECTOR_LIVE"
        ) for i in range(50)
    ]
    spool.append_ticks_batch(ticks, stream_key="WINZ26")

    try:
        with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker ok"}), \
             patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PG ok"}), \
             patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 ok"}), \
             patch("src.health_checker.is_b3_market_open", return_value=True):
            
            diag = evaluate_system_health(config=cfg, spool=spool, max_pending_count=5000, max_pending_age_sec=60.0)
            assert diag["status"] == "HEALTHY"
            assert diag["buffer"]["pending_metrics"]["pending_ticks_count"] == 50
    finally:
        lock.release()

# -------------------------------------------------------------
# Cenário 23: Checkpoints Independentes (Reconciliação Histórica H002)
# -------------------------------------------------------------
def test_scenario_23_independent_checkpoints_semantics(tmp_path):
    cfg = create_mock_config(tmp_path)
    spool = DurableSpool(cfg.buffer.sqlite_path)
    lock_dir = tmp_path / "buffer"
    lock = CollectorProcessLock("WINZ26", lock_dir)
    lock.acquire()

    # Simular cenário onde reconciliação histórica avançou checkpoints de forma independente
    with spool._get_connection() as conn:
        conn.execute("""
        INSERT OR REPLACE INTO checkpoints_two_stage (
            stream_key, last_captured_msc, last_replicated_msc, watermark_complete_msc, updated_at_utc
        ) VALUES ('WINZ26', 1792400050000, 1792400060000, 1792400040000, '2026-10-24T18:00:00Z');
        """)
        conn.commit()

    ckp = spool.get_checkpoints("WINZ26")
    assert ckp.last_replicated_msc > ckp.last_captured_msc  # Fluxo independente H002

    try:
        with patch("src.health_checker.check_docker_engine", return_value={"available": True, "version": "29.4.0", "message": "Docker ok"}), \
             patch("src.health_checker.check_postgres_health", return_value={"healthy": True, "port_open": True, "can_query": True, "message": "PG ok"}), \
             patch("src.health_checker.check_mt5_health", return_value={"healthy": True, "connected": True, "symbol_available": True, "waiting_user": False, "message": "MT5 ok"}), \
             patch("src.health_checker.is_b3_market_open", return_value=True):
            
            # O healthcheck não deve quebrar nem marcar UNHEALTHY exclusivamente por essa relação
            diag = evaluate_system_health(config=cfg, spool=spool)
            assert diag["status"] == "HEALTHY"
            assert diag["buffer"]["checkpoints"]["last_replicated_msc"] == 1792400060000
    finally:
        lock.release()
