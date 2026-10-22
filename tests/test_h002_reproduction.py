import os
from pathlib import Path
from datetime import date, datetime, timezone, timedelta
import pytest
import sqlite3
from unittest.mock import MagicMock, patch

from src.connection.mt5_reader import MT5ReadOnlyReader
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.engine import MarketCollectorEngine
from src.config import load_config
from src.reconciliation.periodic_reconciler import PeriodicHistoricalReconciler
from src.buffer.durable_spool import DurableSpool
from src.domain.models import TradeTick, M2CandleClosed, Checkpoints, compute_tick_event_signature
from src.storage.postgres_writer import PostgresMarketWriter, SafeDropViolationError
from src.reconciliation.tick_reconciler import TickReconciler

def test_reproduce_p0_01_mt5_reader_truncates_subsecond_precision():
    """
    H002-P0-01 Reprodução:
    O leitor converte timestamps numéricos em datetime com precisão de segundos inteiros,
    eliminando os milissegundos do cursor de consulta copy_ticks_from / copy_ticks_range.
    """
    normalizer = MT5TimeNormalizer()
    reader = MT5ReadOnlyReader(time_normalizer=normalizer)
    
    # Timestamp com fração de milissegundo: 09:00:15.789 BRT
    t_msc = 1792573215789
    resolved_dt = reader._resolve_query_datetime(t_msc)
    
    # Comprovação do defeito: microsecond é 0 (truncamento severo em segundos)
    assert resolved_dt.microsecond == 0, "O leitor atual trunca milissegundos para 0!"
    assert int(resolved_dt.timestamp() * 1000) != t_msc, "Perda dos 789 milissegundos no cursor!"

def test_reproduce_p0_02_reconciler_blocked_when_capture_is_stagnant(tmp_path):
    """
    H002-P0-02 Reprodução:
    O limite audit_ceiling_msc usa min(now - safety_lag, last_captured_msc).
    Quando a captura fica estacionada em lote denso, last_captured_msc não avança,
    fazendo audit_ceiling_msc <= from_msc e retornando UP_TO_DATE sem auditar.
    """
    db_file = str(tmp_path / "spool_p0_02.db")
    spool = DurableSpool(db_file)
    
    # Simular checkpoint onde captura ficou estacionada em 10:00:00 (1792576800000)
    # e watermark_complete atingiu o mesmo ponto
    stagnant_msc = 1792576800000
    with spool._get_connection() as conn:
        conn.execute("""
        INSERT INTO checkpoints_two_stage (stream_key, last_captured_msc, last_replicated_msc, watermark_complete_msc, updated_at_utc)
        VALUES ('WINZ26', ?, ?, ?, '2026-10-22T13:00:00+00:00');
        """, (stagnant_msc, stagnant_msc, stagnant_msc))
        conn.commit()
        
    mock_reader = MagicMock()
    mock_reader.is_connected.return_value = True
    
    reconciler = PeriodicHistoricalReconciler(
        reader=mock_reader,
        spool=spool,
        symbol="WINZ26",
        safety_lag_ms=2000
    )
    
    # O tempo atual do servidor avançou para 10:15:00 (+15 min de mercado)
    now_server_msc = stagnant_msc + (15 * 60 * 1000)
    res = reconciler.reconcile_cycle(now_server_msc=now_server_msc)
    
    # Comprovação do defeito: Reconciliador bloqueia e retorna UP_TO_DATE sem chamar fetch_ticks_range!
    assert res["status"] == "UP_TO_DATE", "Reconciliador bloqueou devido ao acoplamento a last_captured_msc!"
    mock_reader.fetch_ticks_range.assert_not_called()

def test_reproduce_p0_03_mt5_reader_returns_empty_list_on_none_failure():
    """
    H002-P0-03 Reprodução:
    copy_ticks_range retornando None (falha interna MT5) é indistinguível de [] (janela vazia válida).
    """
    normalizer = MT5TimeNormalizer()
    reader = MT5ReadOnlyReader(time_normalizer=normalizer)
    
    with patch("src.connection.mt5_reader.mt5") as mock_mt5:
        # Simular falha da C-extension retornando None
        mock_mt5.copy_ticks_range.return_value = None
        mock_mt5.last_error.return_value = (-10004, "Terminal internal error")
        
        ticks = reader.fetch_ticks_range("WINZ26", 1792573200000, 1792573260000)
        # Comprovação do defeito: Retorna lista vazia [], mascarando a falha
        assert ticks == [], "Retorna [] idêntico a sucesso sem dados!"

def test_reproduce_p1_04_sqlite_spool_accounting_returns_attempted_instead_of_inserted(tmp_path):
    """
    H002-P1-04 Reprodução:
    append_ticks_batch usa INSERT OR IGNORE, mas retorna len(records) (tentados, não inseridos).
    """
    db_file = str(tmp_path / "spool_p1_04.db")
    spool = DurableSpool(db_file)
    
    tick = TradeTick(
        symbol="WINZ26",
        time_msc=1792573200000,
        time_utc=datetime.now(timezone.utc),
        price=212900.0,
        volume_real=10.0,
        flags=1080,
        trade_side="BUY",
        occurrence_ordinal=1,
        tick_fingerprint="fp_dup_test_1"
    )
    
    # Primeira inserção: 1 tick inserido
    res1 = spool.append_ticks_batch([tick], stream_key="WINZ26")
    assert res1 == 1
    
    # Segunda inserção idêntica (duplicata pura)
    res2 = spool.append_ticks_batch([tick], stream_key="WINZ26")
    
    # Comprovação do defeito: Retorna 1 mesmo tendo inserido 0 registros novos!
    assert res2 == 1, "append_ticks_batch atual reporta 1 inserção para um lote 100% duplicado!"
    with spool._get_connection() as conn:
        count = conn.execute("SELECT count(*) FROM spool_ticks;").fetchone()[0]
        assert count == 1, "De fato existe apenas 1 registro no banco!"

def test_reproduce_p1_05_postgres_writer_accounting_returns_len_records():
    """
    H002-P1-05 Reprodução:
    write_ticks_batch retorna len(records) incondicionalmente com ON CONFLICT DO NOTHING.
    """
    writer = PostgresMarketWriter(database="market_data_test")
    mock_cursor = MagicMock()
    mock_cursor.mogrify.return_value = b"INSERT INTO dummy VALUES (1);"
    mock_cursor.__enter__.return_value = mock_cursor
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    with patch.object(writer, "get_connection", return_value=mock_conn):
        ticks = [
            TradeTick(
                symbol="WINZ26",
                time_msc=1792573200000,
                time_utc=datetime.now(timezone.utc),
                price=212900.0,
                volume_real=10.0,
                flags=1080,
                trade_side="BUY",
                occurrence_ordinal=1,
                tick_fingerprint="fp_pg_dup_1"
            )
        ]
        res = writer.write_ticks_batch(ticks)
        # Comprovação do defeito: Retorna 1 sem consultar se houve inserção ou conflito ignorado
        assert res == 1

def test_reproduce_p1_06_candle_is_closed_based_solely_on_clock(tmp_path):
    """
    H002-P1-06 Reprodução:
    sync_m2_candles marca is_closed=True estritamente pela passagem do tempo
    (candle_sec + 120 <= now_server_sec) sem verificar se os ticks correspondentes
    foram capturados, reconciliados ou persistidos.
    """
    db_file = str(tmp_path / "spool_p1_06.db")
    spool = DurableSpool(db_file)
    mock_reader = MagicMock()
    mock_writer = MagicMock()
    
    # Retornar candle de 09:00:00 (fechamento em 09:02:00)
    mock_reader.fetch_rates_range.return_value = [
        {
            "time": 1792573200, # 09:00:00
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
    
    # Simular relógio em 09:05:00 (mais de 120s decorridos)
    with patch.object(engine.time_normalizer, "utc_datetime_to_server_msc", return_value=(1792573200 + 300) * 1000):
        synced = engine.sync_m2_candles()
        
    assert synced == 1
    # Comprovação do defeito: Candle foi gravado no banco com is_closed=True sem NENHUM tick existente no spool!
    with spool._get_connection() as conn:
        ticks_count = conn.execute("SELECT count(*) FROM spool_ticks;").fetchone()[0]
        assert ticks_count == 0, "Zero ticks no spool!"
        candle_row = conn.execute("SELECT open, close, status FROM spool_candles_m2;").fetchone()
        assert candle_row is not None
        mock_writer.write_candle_m2.assert_called_once()
        written_candle = mock_writer.write_candle_m2.call_args[0][0]
        assert written_candle.is_closed is True, "Candle rotulado como is_closed=True sem comprovação de cobertura de ticks!"

def test_reproduce_p1_07_safe_drop_returns_dropped_on_sql_exception():
    """
    H002-P1-07 Reprodução:
    evaluate_safe_drop_partition captura Exception no drop_sql e ainda retorna action: DROPPED!
    """
    mock_archiver = MagicMock()
    mock_archiver.validate_archive_integrity.return_value = True
    mock_archiver.compute_file_sha256.return_value = "dummy_sha"
    mock_archiver.backup_dir = Path("/tmp/mock_backup")
    mock_archiver.archive_dir = Path("/tmp/mock_archive")
    
    writer = PostgresMarketWriter(database="market_data_test", archiver=mock_archiver)
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Simular falha de permissão / banco no DROP
    mock_cursor.execute.side_effect = Exception("permission denied to drop table")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    with patch.object(writer, "get_connection", return_value=mock_conn), \
         patch("pathlib.Path.exists", return_value=True), \
         patch("src.storage.postgres_writer.get_cutoff_trading_date_for_retention", return_value=date(2026, 10, 1)):
        
        # Data anterior ao corte (elegível para drop)
        res = writer.evaluate_safe_drop_partition(date(2026, 8, 1))
        
        # Comprovação do defeito: Reporta DROPPED mesmo tendo levantado Exception!
        assert res["action"] == "DROPPED", "evaluate_safe_drop_partition reporta DROPPED mesmo após falha na execução do DROP!"

def test_reproduce_p1_08_flags_mutations_and_signature_divergence():
    """
    H002-P1-08 Reprodução:
    TickReconciler mascara apenas bit 256 (~256).
    Se o MT5 retornar flag com bit 1024 no histórico vs sem 1024 no streaming,
    a assinatura de evento diverge e gera duplicação artificial de ticks.
    """
    time_msc = 1792573200000
    price = 212900.0
    vol = 10.0
    side = "BUY"
    
    flag_realtime = 40    # 32 (BUY) + 8 (LAST)
    flag_historical = 1064 # 1024 (TRANSPORT) + 32 (BUY) + 8 (LAST)
    
    # Na implementação atual:
    can_flags_rt = flag_realtime & ~256
    can_flags_hist = flag_historical & ~256
    
    sig_rt = compute_tick_event_signature(time_msc, price, vol, can_flags_rt, side)
    sig_hist = compute_tick_event_signature(time_msc, price, vol, can_flags_hist, side)
    
    # Comprovação do defeito: As assinaturas divergem por causa do bit 1024!
    assert sig_rt != sig_hist, "Assinaturas divergem gerando duplicatas artificiais entre streaming e histórico!"
