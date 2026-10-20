import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock
from src.config import AppConfig, CollectorSettings, BufferConfig, PostgresConfig, ParquetConfig

from src.buffer.durable_spool import DurableSpool
from src.engine import MarketCollectorEngine
from src.domain.models import TradeTick
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.reconciliation.tick_reconciler import (
    TICK_FLAG_LAST, TICK_FLAG_VOLUME, TICK_FLAG_BUY
)

def test_late_event_recovery_8s_deterministic(tmp_path):
    """
    Teste determinístico do Gate Final de Implantação:
    Cenário:
    - O coletor recebe negócios em tempo real até 14:00:08.
    - Checkpoint last_captured_msc (W_durable) já está em 14:00:08.
    - Conexão do MT5 está ativa e contínua (is_connected=True, sem alerta de gap).
    - Um negócio das 14:00:01 aparece no histórico do MT5 com atraso de 7-8 segundos.
    - O lookback de 2s do capture_step NÃO alcança o evento (14:00:08 - 2s = 14:00:06).
    - O PeriodicHistoricalReconciler varre a janela retrospectiva:
      1. Detecta e recupera o negócio tardio omitido das 14:00:01.
      2. Preserva rigorosamente W_durable (sem regressão de checkpoints).
      3. Avança W_complete estritamente até a cobertura auditada (14:00:06, respeitando safety_lag).
      4. Garante idempotência e ausência absoluta de duplicação.
    """
    db_file = tmp_path / "spool_test.db"
    normalizer = MT5TimeNormalizer()
    spool = DurableSpool(str(db_file), time_normalizer=normalizer)

    # 1. Simular estado em 14:00:08 (1792418408000 msc)
    # Ticks normais já capturados pelo realtime stream às 14:00:05 e 14:00:08
    t_14_00_05_msc = 1792418405000
    t_14_00_08_msc = 1792418408000
    t_14_00_01_msc = 1792418401000 # O negócio que chegará tardiamente

    from src.reconciliation.tick_reconciler import TickReconciler
    rec = TickReconciler(time_normalizer=normalizer)
    realtime_ticks = rec.reconcile_raw_ticks([
        {
            "time_msc": t_14_00_05_msc,
            "price": 120000.0,
            "volume_real": 5.0,
            "flags": TICK_FLAG_LAST | TICK_FLAG_VOLUME | TICK_FLAG_BUY,
        },
        {
            "time_msc": t_14_00_08_msc,
            "price": 120005.0,
            "volume_real": 10.0,
            "flags": TICK_FLAG_LAST | TICK_FLAG_VOLUME | TICK_FLAG_BUY,
        }
    ], symbol="WINZ26", provenance="REALTIME_STREAM")

    # Persiste os negócios regulares recebidos no realtime
    spool.append_ticks_batch(realtime_ticks, stream_key="WINZ26")


    ckp_initial = spool.get_checkpoints("WINZ26")
    assert ckp_initial.last_captured_msc == t_14_00_08_msc
    assert ckp_initial.watermark_complete_msc == 0

    # 2. Mock do MT5 Reader
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True

    # Histórico do MT5 na janela contém o negócio prévio de 14:00:05 e o negócio tardio de 14:00:01
    raw_mt5_ticks = [
        {
            "time_msc": t_14_00_01_msc,
            "price": 119995.0,
            "volume_real": 15.0,
            "flags": TICK_FLAG_LAST | TICK_FLAG_VOLUME | TICK_FLAG_BUY,
        },
        {
            "time_msc": t_14_00_05_msc,
            "price": 120000.0,
            "volume_real": 5.0,
            "flags": TICK_FLAG_LAST | TICK_FLAG_VOLUME | TICK_FLAG_BUY,
        }
    ]

    def mock_fetch_range(symbol, from_msc, to_msc):
        # Retorna apenas ticks dentro do range consultado
        return [t for t in raw_mt5_ticks if from_msc <= t["time_msc"] <= to_msc]

    mock_reader.fetch_ticks_range.side_effect = mock_fetch_range

    config = AppConfig(
        symbol="WINZ26",
        timezones={"mt5_server": "America/Sao_Paulo", "market": "America/Sao_Paulo"},
        collector=CollectorSettings(
            read_only=True,
            allow_orders=False,
            poll_interval_ms=100,
            gap_inactivity_threshold_seconds=15,
            lookback_ms=2000
        ),
        buffer=BufferConfig(
            sqlite_path=str(db_file),
            journal_mode="WAL",
            synchronous="FULL",
            batch_size=1000
        ),
        postgres=PostgresConfig(
            host="localhost",
            port=5435,
            database="market_data_sandbox",
            user="admin",
            password="admin_password",
            retention_trading_days=5
        ),
        parquet=ParquetConfig(
            archive_dir="data/archive",
            backup_dir="data/backup",
            compression="SNAPPY",
            compression_level=1,
            ticks_retention_years=5,
            candles_m2_retention_years=5
        )
    )


    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        time_normalizer=normalizer
    )

    # 3. Demonstrar que o capture_step com lookback de 2s NÃO alcança o evento de 14:00:01
    lookback_ms = 2000
    capture_from = ckp_initial.last_captured_msc - lookback_ms
    assert capture_from == 1792418406000 # 14:00:06
    assert t_14_00_01_msc < capture_from # 14:00:01 < 14:00:06 (fora da janela do capture_step!)

    # 4. Executar o ciclo do PeriodicHistoricalReconciler em 14:00:08
    # Safety lag de 2 segundos -> limite superior de auditoria é 14:00:06
    reconcile_res = engine.periodic_reconcile_step(now_server_msc=t_14_00_08_msc)

    # 5. Asserções de Validação Formal
    assert reconcile_res["status"] == "RECONCILED"
    assert reconcile_res["recovered_count"] == 1 # Exatamente o negócio atrasado de 14:00:01 recuperado
    assert reconcile_res["w_complete"] == 1792418406000 # Avançou exatamente até 14:00:06 (now - 2s)

    # 6. Validar estado do banco e checkpoints no Spool
    ckp_after = spool.get_checkpoints("WINZ26")
    
    # Checkpoint de captura (W_durable) NÃO regrediu!
    assert ckp_after.last_captured_msc == t_14_00_08_msc

    # Checkpoint de completude auditada (W_complete) avançou até a cobertura verificada
    assert ckp_after.watermark_complete_msc == 1792418406000
    assert ckp_after.watermark_complete_msc <= t_14_00_08_msc - lookback_ms

    # Verificar dados no spool SQLite
    with spool._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT time_msc, price, volume_real, provenance, status FROM spool_ticks ORDER BY time_msc ASC;")
        rows = cursor.fetchall()

    # Devem existir exatamente 3 ticks: 14:00:01 (recuperado), 14:00:05 (original), 14:00:08 (original)
    assert len(rows) == 3
    assert rows[0]["time_msc"] == t_14_00_01_msc
    assert rows[0]["price"] == 119995.0
    assert rows[0]["volume_real"] == 15.0
    assert rows[0]["provenance"] == "RECOVERED_HISTORICAL_MT5"
    assert rows[0]["status"] == "PENDING"

    assert rows[1]["time_msc"] == t_14_00_05_msc
    assert rows[1]["provenance"] == "REALTIME_STREAM"

    assert rows[2]["time_msc"] == t_14_00_08_msc
    assert rows[2]["provenance"] == "REALTIME_STREAM"

    # 7. Teste de Idempotência: re-executar sem novos ticks não duplica nem altera checkpoints
    reconcile_res_2 = engine.periodic_reconcile_step(now_server_msc=t_14_00_08_msc)
    assert reconcile_res_2["status"] == "UP_TO_DATE" # Já coberto até 14:00:06
    assert reconcile_res_2["recovered_count"] == 0

    with spool._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM spool_ticks;")
        assert cursor.fetchone()["cnt"] == 3
