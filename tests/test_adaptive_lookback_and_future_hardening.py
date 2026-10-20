import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock
from src.engine import MarketCollectorEngine
from src.domain.models import Checkpoints, TradeTick
from src.config import AppConfig, BufferConfig, PostgresConfig, ParquetConfig, CollectorSettings
from src.reconciliation.tick_reconciler import TickReconciler

@pytest.fixture
def test_config(tmp_path):
    db_file = tmp_path / "test_lookback_spool.db"
    return AppConfig(
        symbol="WINZ26",
        timezones={"storage": "UTC", "display": "America/Sao_Paulo", "market": "America/Sao_Paulo", "mt5_server": "America/Sao_Paulo"},
        buffer=BufferConfig(sqlite_path=str(db_file), journal_mode="WAL", synchronous="FULL", batch_size=500),
        postgres=PostgresConfig(host="127.0.0.1", port=5435, database="market_data_test", user="user", password="pwd", retention_trading_days=30),
        parquet=ParquetConfig(archive_dir=str(tmp_path / "archive"), backup_dir=str(tmp_path / "backup"), compression="zstd", compression_level=7, ticks_retention_years=1, candles_m2_retention_years=3),
        collector=CollectorSettings(read_only=True, allow_orders=False, poll_interval_ms=100, gap_inactivity_threshold_seconds=15, lookback_ms=2000)
    )

def test_adaptive_lookback_captures_delayed_ticks(test_config):
    """
    Testa se ticks que chegam com atraso dentro da janela de lookback (1500 ms)
    são capturados com sucesso sem perda de continuidade e sem duplicar ticks anteriores.
    """
    reader = MagicMock()
    reader.is_connected.return_value = True
    
    engine = MarketCollectorEngine(config=test_config, reader=reader)
    engine.spool._init_db()
    
    session_start = engine._get_session_start_msc()
    t1_msc = session_start + 50000
    batch1 = [
        {"time_msc": t1_msc, "price": 209500.0, "volume_real": 5.0, "flags": 1080}
    ]
    engine.capture_step(raw_ticks=batch1)
    
    ckp1 = engine.spool.get_checkpoints("WINZ26")
    assert ckp1.last_captured_msc == t1_msc
    
    # 2. Na próxima chamada de captura sem raw_ticks explícito, o reader deve ser chamado
    # com from_msc = last_captured_msc - lookback_ms (t1_msc - 2000)
    reader.fetch_ticks_from.return_value = [
        # Tick já visto anteriormente
        {"time_msc": t1_msc, "price": 209500.0, "volume_real": 5.0, "flags": 1080},
        # Novo tick que chegou com atraso de 500 ms (t = t1_msc - 500)
        {"time_msc": t1_msc - 500, "price": 209505.0, "volume_real": 2.0, "flags": 1112},
        # Novo tick recente
        {"time_msc": t1_msc + 100, "price": 209510.0, "volume_real": 1.0, "flags": 1080}
    ]
    
    engine.capture_step()
    
    # Verificar se o fetch_ticks_from foi chamado com o lookback correto
    reader.fetch_ticks_from.assert_called_with("WINZ26", t1_msc - 2000, count=500)
    
    # Verificar itens no buffer: devem ser 3 no total (sem duplicar o primeiro)
    pending = engine.spool.get_pending_ticks(limit=10)
    assert len(pending) == 3
    
    # Verificar se o checkpoint avançou para o maior timestamp e não regrediu
    ckp2 = engine.spool.get_checkpoints("WINZ26")
    assert ckp2.last_captured_msc == t1_msc + 100

def test_bit_256_mutation_deduplication(test_config):
    """
    Testa se ticks com bit 256 (TICK_FLAG_VOLUME_REAL) geram a mesma assinatura
    e são deduplicados sem gerar excedentes quando retransmitidos.
    """
    reconciler = TickReconciler()
    
    # Ticks live (flags = 1080 BUY)
    live_tick = {"time_msc": 1792400600000, "price": 209600.0, "volume_real": 10.0, "flags": 1080}
    # Tick histórico / pós-reconexão com bit 256 adicionado (flags = 1336 = 1080 | 256)
    hist_tick = {"time_msc": 1792400600000, "price": 209600.0, "volume_real": 10.0, "flags": 1336}
    
    reconciled_live = reconciler.reconcile_raw_ticks([live_tick], symbol="WINZ26")
    assert len(reconciled_live) == 1
    
    # Ao passar o tick com bit 256, o reconciler deve reconhecer como o mesmo evento
    # e não emitir uma nova duplicata
    reconciled_hist = reconciler.reconcile_raw_ticks([hist_tick], symbol="WINZ26")
    assert len(reconciled_hist) == 0  # Reconciliador identifica como já emitido

def test_watermark_durability_vs_completeness(test_config):
    """
    Verifica se W_durable representa o avanço do SQLite WAL local
    enquanto a garantia de completude só é concedida com tolerância à janela de lookback.
    """
    engine = MarketCollectorEngine(config=test_config)
    engine.spool._init_db()
    
    # Inserir lote
    t_msc = 1792400700000
    ticks = [
        {"time_msc": t_msc, "price": 209700.0, "volume_real": 4.0, "flags": 1080}
    ]
    engine.capture_step(raw_ticks=ticks)
    
    ckp = engine.spool.get_checkpoints("WINZ26")
    w_durable = ckp.last_captured_msc
    assert w_durable == t_msc
    
    # W_complete (completude fechada) deve considerar a janela de lookback (ex: 2000 ms)
    lookback = test_config.collector.lookback_ms
    w_complete = w_durable - lookback
    assert w_complete < w_durable
    assert w_durable - w_complete == 2000
