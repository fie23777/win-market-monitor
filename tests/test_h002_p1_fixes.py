"""
Testes de Homologação da Fase B (P1-05 a P1-08) - H002
win-market-monitor - WINZ26
"""

import pytest
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.config import load_config
from src.domain.models import (
    TradeTick,
    M2CandleClosed,
    ECONOMIC_FLAGS_MASK,
    compute_tick_event_signature,
    compute_tick_fingerprint
)
from src.storage.postgres_writer import PostgresMarketWriter, SafeDropViolationError
from src.storage.parquet_archiver import ParquetArchiver
from src.buffer.durable_spool import DurableSpool
from src.engine import MarketCollectorEngine
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.reconciliation.tick_reconciler import TickReconciler


# ==============================================================================
# H002-P1-05: Contabilização Precisa no PostgreSQL (execute_values + ON CONFLICT)
# ==============================================================================

def test_postgres_writer_accounting_returns_exact_inserted_count(test_postgres_writer):
    """
    H002-P1-05:
    Comprova que write_ticks_batch retorna a contagem exata de linhas inseridas,
    retornando 0 em caso de duplicatas puras ignoradas por ON CONFLICT DO NOTHING.
    """
    writer = test_postgres_writer
    t_utc = datetime(2026, 10, 23, 14, 0, 0, tzinfo=timezone.utc)
    
    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1792700000000 + i * 10,
            time_utc=t_utc + timedelta(milliseconds=i * 10),
            price=215000.0 + i * 5.0,
            volume_real=1.0,
            flags=40,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint=f"fp_h002_p1_05_{i}",
            provenance="REALTIME_STREAM"
        )
        for i in range(10)
    ]

    try:
        # 1. Primeira inserção: 10 novos ticks
        inserted1 = writer.write_ticks_batch(ticks)
        assert inserted1 == 10
        assert writer.last_batch_metrics["attempted"] == 10
        assert writer.last_batch_metrics["inserted"] == 10
        assert writer.last_batch_metrics["duplicates"] == 0

        # 2. Segunda inserção (idêntica): 0 inserções, 10 duplicatas ignoradas
        inserted2 = writer.write_ticks_batch(ticks)
        assert inserted2 == 0, "Lote 100% duplicado deve retornar 0 inserções!"
        assert writer.last_batch_metrics["attempted"] == 10
        assert writer.last_batch_metrics["inserted"] == 0
        assert writer.last_batch_metrics["duplicates"] == 10

        # 3. Lote misto (10 duplicatas + 5 novos)
        new_ticks = [
            TradeTick(
                symbol="WINZ26",
                time_msc=1792700000000 + (10 + i) * 10,
                time_utc=t_utc + timedelta(milliseconds=(10 + i) * 10),
                price=215000.0,
                volume_real=2.0,
                flags=40,
                trade_side="BUY",
                occurrence_ordinal=1,
                tick_fingerprint=f"fp_h002_p1_05_{10 + i}",
                provenance="REALTIME_STREAM"
            )
            for i in range(5)
        ]
        mixed_batch = ticks + new_ticks
        inserted3 = writer.write_ticks_batch(mixed_batch)
        assert inserted3 == 5, "Lote misto deve retornar exatamente a contagem de novas inserções!"
        assert writer.last_batch_metrics["inserted"] == 5
        assert writer.last_batch_metrics["duplicates"] == 10

    finally:
        with writer.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM market_ticks_raw WHERE tick_fingerprint LIKE 'fp_h002_p1_05_%';")
            conn.commit()


# ==============================================================================
# H002-P1-06: Fechamento Seguro de Candles M2 por Cobertura de Ticks
# ==============================================================================

def test_candle_m2_is_provisional_when_tick_coverage_is_not_proven(tmp_path):
    """
    H002-P1-06:
    sync_m2_candles NÃO rotula candle como is_closed=True se o relógio de mercado passou
    mas a cobertura temporal de ticks (W_durable / W_complete) ainda não alcançou o encerramento do candle.
    """
    db_file = str(tmp_path / "spool_p1_06_test.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_writer = MagicMock()

    # Candle das 09:00:00 BRT (fechamento às 09:02:00 BRT)
    candle_start_sec = 1792573200
    mock_reader.fetch_rates_range.return_value = [
        {
            "time": candle_start_sec,
            "open": 212900.0,
            "high": 212950.0,
            "low": 212880.0,
            "close": 212910.0,
            "tick_volume": 500,
            "spread": 5,
            "real_volume": 1500.0,
        }
    ]

    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        writer=mock_writer
    )

    # Simular relógio em 09:05:00 BRT (+300s após abertura)
    # Mas checkpoints do spool estão em 0 (sem ticks capturados/reconciliados)
    with patch.object(engine.time_normalizer, "utc_datetime_to_server_msc", return_value=(candle_start_sec + 300) * 1000):
        synced = engine.sync_m2_candles()

    assert synced == 1
    mock_writer.write_candle_m2.assert_called_once()
    written_candle = mock_writer.write_candle_m2.call_args[0][0]

    # COMPROVAÇÃO DO FIX P1-06: Candle gravado como PROVISÓRIO (is_closed=False)
    assert written_candle.is_closed is False, "Candle sem cobertura de ticks deve ser provisório!"
    assert written_candle.provenance == "PROVISIONAL_AWAITING_TICK_COVERAGE"


def test_candle_m2_promoted_to_closed_when_ticks_are_covered(tmp_path):
    """
    H002-P1-06:
    Quando os checkpoints comprovam que a captura durável (W_durable) ou a auditoria (W_complete)
    alcançaram o encerramento do candle, ele é gravado como is_closed=True.
    """
    db_file = str(tmp_path / "spool_p1_06_promoted.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_writer = MagicMock()

    candle_start_sec = 1792573200 # 09:00:00
    candle_close_msc = (candle_start_sec + 120) * 1000 # 09:02:00.000

    # Configurar checkpoints comprovando cobertura até 09:03:00 (+180s)
    with spool._get_connection() as conn:
        conn.execute("""
        INSERT INTO checkpoints_two_stage (stream_key, last_captured_msc, last_replicated_msc, watermark_complete_msc, updated_at_utc)
        VALUES ('WINZ26', ?, ?, ?, '2026-10-23T12:00:00+00:00');
        """, ((candle_start_sec + 180) * 1000, candle_close_msc, candle_close_msc))
        conn.commit()

    mock_reader.fetch_rates_range.return_value = [
        {
            "time": candle_start_sec,
            "open": 212900.0,
            "high": 212950.0,
            "low": 212880.0,
            "close": 212910.0,
            "tick_volume": 500,
            "spread": 5,
            "real_volume": 1500.0,
        }
    ]

    config = load_config()
    engine = MarketCollectorEngine(
        config=config,
        reader=mock_reader,
        spool=spool,
        writer=mock_writer
    )

    with patch.object(engine.time_normalizer, "utc_datetime_to_server_msc", return_value=(candle_start_sec + 300) * 1000):
        synced = engine.sync_m2_candles()

    assert synced == 1
    written_candle = mock_writer.write_candle_m2.call_args[0][0]
    assert written_candle.is_closed is True, "Candle com cobertura temporal comprovada deve ser fechado!"
    assert written_candle.provenance == "RECOVERED_HISTORICAL_MT5"


# ==============================================================================
# H002-P1-07: Comportamento Estrito Fail-Closed no Safe Drop e Partições
# ==============================================================================

def test_ensure_daily_partition_fail_closed_raises_on_sql_error():
    """
    H002-P1-07:
    ensure_daily_partition propaga exceção caso o banco de dados falhe ao criar a partição.
    """
    writer = PostgresMarketWriter(database="market_data_test")
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_cur.execute.side_effect = Exception("Disk full or connection broken")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch.object(writer, "get_connection", return_value=mock_conn):
        with pytest.raises(RuntimeError, match="Falha fail-closed na criação da partição"):
            writer.ensure_daily_partition(date(2026, 10, 23))


def test_safe_drop_fail_closed_raises_on_drop_failure():
    """
    H002-P1-07:
    evaluate_safe_drop_partition levanta SafeDropViolationError caso o DROP SQL falhe,
    nunca retornando 'DROPPED' falsamente.
    """
    mock_archiver = MagicMock()
    mock_archiver.validate_archive_integrity.return_value = True
    mock_archiver.compute_file_sha256.return_value = "dummy_sha"
    mock_archiver.backup_dir = Path("/tmp/mock_backup")
    mock_archiver.archive_dir = Path("/tmp/mock_archive")

    writer = PostgresMarketWriter(database="market_data_test", archiver=mock_archiver)

    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_cur = MagicMock()
    mock_cur.execute.side_effect = Exception("Permission denied to drop table")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with patch.object(writer, "get_connection", return_value=mock_conn), \
         patch("pathlib.Path.exists", return_value=True), \
         patch("src.storage.postgres_writer.get_cutoff_trading_date_for_retention", return_value=date(2026, 10, 1)):
        
        with pytest.raises(SafeDropViolationError, match="Falha na execução SQL do DROP TABLE"):
            writer.evaluate_safe_drop_partition(date(2026, 8, 1))


# ==============================================================================
# H002-P1-08: Identidade Canônica Isolando Bits Econômicos (ECONOMIC_FLAGS_MASK)
# ==============================================================================

def test_economic_flags_mask_canonical_identity():
    """
    H002-P1-08:
    Comprova que ECONOMIC_FLAGS_MASK (104) isola os bits econômicos (8|32|64),
    fazendo com que mutações de bits de transporte MT5 (1024, 256, 16) gerem
    exatamente a MESMA assinatura e fingerprint sem duplicatas artificiais.
    """
    assert ECONOMIC_FLAGS_MASK == 104

    time_msc = 1792573200000
    price = 212900.0
    vol = 10.0
    side = "BUY"

    # Diferentes representações do mesmo trade emitidas pelo MT5:
    flag_clean_rt = 40          # 32 (BUY) + 8 (LAST)
    flag_with_transport = 1064  # 1024 (TRANSPORT) + 32 (BUY) + 8 (LAST)
    flag_with_vol_real = 296    # 256 (VOL_REAL) + 32 (BUY) + 8 (LAST)
    flag_with_both = 1320       # 1024 + 256 + 32 + 8

    sig_clean = compute_tick_event_signature(time_msc, price, vol, flag_clean_rt, side)
    sig_transport = compute_tick_event_signature(time_msc, price, vol, flag_with_transport, side)
    sig_vol_real = compute_tick_event_signature(time_msc, price, vol, flag_with_vol_real, side)
    sig_both = compute_tick_event_signature(time_msc, price, vol, flag_with_both, side)

    # COMPROVAÇÃO DO FIX P1-08: Todas as assinaturas são idênticas
    assert sig_clean == sig_transport == sig_vol_real == sig_both

    # Fingerprints gerados para a primeira ocorrência também são idênticos
    fp_clean = compute_tick_fingerprint(time_msc, sig_clean, 1)
    fp_transport = compute_tick_fingerprint(time_msc, sig_transport, 1)
    assert fp_clean == fp_transport

    # O objeto TradeTick retém a flag original bruta para auditoria forense
    tick_rt = TradeTick(
        symbol="WINZ26", time_msc=time_msc, time_utc=datetime.now(timezone.utc),
        price=price, volume_real=vol, flags=flag_clean_rt, trade_side=side
    )
    tick_hist = TradeTick(
        symbol="WINZ26", time_msc=time_msc, time_utc=datetime.now(timezone.utc),
        price=price, volume_real=vol, flags=flag_with_transport, trade_side=side
    )
    assert tick_rt.flags == 40
    assert tick_hist.flags == 1064
    assert tick_rt.event_signature == tick_hist.event_signature
    assert tick_rt.tick_fingerprint == tick_hist.tick_fingerprint


def test_tick_reconciler_deduplicates_across_streaming_and_historical_mutations():
    """
    H002-P1-08:
    Reconciliador recebe trade no streaming com flag 40 e posteriormente no histórico com flag 1064.
    Deduplicação determinística reconhece o mesmo evento econômico e emite 0 duplicatas.
    """
    reconciler = TickReconciler()
    time_msc = 1792573200100

    # 1. Lote recebido via streaming ao vivo (flag 40)
    rt_ticks = [
        {"time_msc": time_msc, "price": 212900.0, "volume_real": 5.0, "flags": 40}
    ]
    emitted_rt = reconciler.reconcile_raw_ticks(rt_ticks, symbol="WINZ26", provenance="REALTIME_STREAM")
    assert len(emitted_rt) == 1
    assert emitted_rt[0].flags == 40
    assert emitted_rt[0].provenance == "REALTIME_STREAM"

    # 2. Lote recebido retrospectivamente via histórico com bit 1024 injetado (flag 1064)
    hist_ticks = [
        {"time_msc": time_msc, "price": 212900.0, "volume_real": 5.0, "flags": 1064}
    ]
    emitted_hist = reconciler.reconcile_raw_ticks(hist_ticks, symbol="WINZ26", provenance="RECOVERED_HISTORICAL_MT5")
    
    # COMPROVAÇÃO DO FIX P1-08: Zero duplicatas emitidas!
    assert len(emitted_hist) == 0, "O mesmo trade com bits de transporte adicionais foi corretamente deduplicado!"
