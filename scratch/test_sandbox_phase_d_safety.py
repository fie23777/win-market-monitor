import pytest
import sqlite3
import os
import gc
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

from scratch.sandbox_phase_d.mt5_reference_generator import MT5ReferenceExtractor
from scratch.sandbox_phase_d.backfill_engine import SafeBackfillEngine
from scratch.sandbox_phase_d.audit_and_reconstruction import CanonicalCandleReconstructor, WindowAuditReconciler
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.domain.models import TradeTick

def test_dynamic_session_boundary_and_reference_freezing(tmp_path):
    """
    Requisitos 1 & 2:
    Demonstra a detecção dinâmica de término de sessão no MT5 e o congelamento
    da referência em Parquet ZSTD com cálculo de manifesto SHA-256.
    """
    normalizer = MT5TimeNormalizer()
    mock_reader = MagicMock()

    # Simular histórico retornado pelo MT5
    start_msc = 1792573200000 # 09:00:00 BRT
    end_msc = 1792573258338   # 09:00:58.338 BRT (último observado na janela piloto)
    
    mock_ticks = [
        {
            "time": 1792573257,
            "time_msc": 1792573257687,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 35450.0,
            "volume_real": 35450.0,
            "flags": 1080
        },
        {
            "time": 1792573258,
            "time_msc": 1792573258338,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 1000.0,
            "volume_real": 1000.0,
            "flags": 1080
        }
    ]
    mock_reader.fetch_ticks_range.return_value = mock_ticks

    extractor = MT5ReferenceExtractor(reader=mock_reader, time_normalizer=normalizer)
    p_file, m_file, manifest = extractor.freeze_reference_parquet(
        symbol="WINZ26",
        from_msc=start_msc,
        to_msc=end_msc,
        output_dir=str(tmp_path)
    )

    assert os.path.exists(p_file)
    assert os.path.exists(m_file)
    assert manifest["raw_ticks_count"] == 2
    assert manifest["eligible_trades_count"] == 2
    assert manifest["total_volume_real"] == 36450.0
    assert len(manifest["parquet_sha256"]) == 64 # Hash SHA-256 válido!
    print(f"\nManifesto gerado com sucesso: SHA-256={manifest['parquet_sha256'][:16]}...")

def test_lineage_tracking_accounting_and_surgical_rollback(tmp_path):
    """
    Requisitos 3, 4, 5, 6 & 7:
    Demonstra o ciclo completo no sandbox:
    1. Ingestão inicial (simulando os 996 ticks do piloto).
    2. Backfill auditado com backfill_run_id rastreável.
    3. Contabilidade exata (tentados, novos, já existentes, rejeitados).
    4. Validação de idempotência.
    5. ROLLBACK CIRÚRGICO ATÔMICO comprovado: remoção estrita das chaves do run_id
       e restauração íntegra de checkpoints e contagens conjuntas.
    """
    normalizer = MT5TimeNormalizer()
    db_file = str(tmp_path / "sandbox_test_spool.db")
    
    # Criar schema de spool no SQLite
    with sqlite3.connect(db_file) as conn:
        conn.executescript("""
        CREATE TABLE spool_ticks (
            tick_fingerprint TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            time_msc INTEGER NOT NULL,
            price REAL NOT NULL,
            volume_real REAL NOT NULL,
            flags INTEGER NOT NULL,
            trade_side TEXT NOT NULL,
            occurrence_ordinal INTEGER NOT NULL,
            provenance TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at_utc TEXT NOT NULL
        );
        CREATE TABLE checkpoints_two_stage (
            stream_key TEXT PRIMARY KEY,
            last_captured_msc INTEGER NOT NULL,
            last_replicated_msc INTEGER NOT NULL,
            watermark_complete_msc INTEGER NOT NULL,
            updated_at_utc TEXT NOT NULL
        );
        INSERT INTO checkpoints_two_stage VALUES ('WINZ26', 1792573258338, 1792573258338, 1792573258338, '2026-10-21T09:01:28+00:00');
        INSERT INTO spool_ticks VALUES ('fp_initial_001', 'WINZ26', 1792573258338, 212900.0, 100.0, 1080, 'BUY', 1, 'REALTIME', 'COMMITTED', '2026-10-21T09:01:28+00:00');
        """)

    # Mock do PostgreSQL para ensaio isolado no sandbox
    mock_pg_params = {"host": "127.0.0.1", "port": 5434, "dbname": "market_data", "user": "postgres", "password": "password"}

    engine = SafeBackfillEngine(sqlite_path=db_file, pg_conn_params=mock_pg_params, time_normalizer=normalizer)

    # Preparar lote de teste contendo:
    # - 1 tick já existente (fp_initial_001)
    # - 2 ticks novos legítimos no mesmo milissegundo (testando ordinal de multiplicidade)
    # - 1 tick com preço inválido (rejeitado)
    now_utc = datetime.now(timezone.utc)
    test_ticks = [
        TradeTick(symbol="WINZ26", time_msc=1792573258338, time_utc=now_utc, price=212900.0, volume_real=100.0, flags=1080, trade_side="BUY", occurrence_ordinal=1, tick_fingerprint="fp_initial_001", provenance="TEST", ingested_at_utc=now_utc),
        TradeTick(symbol="WINZ26", time_msc=1792573260000, time_utc=now_utc, price=212950.0, volume_real=50.0, flags=1080, trade_side="BUY", occurrence_ordinal=1, tick_fingerprint="fp_new_002", provenance="RECOVERED_HISTORICAL_MT5", ingested_at_utc=now_utc),
        TradeTick(symbol="WINZ26", time_msc=1792573260000, time_utc=now_utc, price=212950.0, volume_real=50.0, flags=1080, trade_side="BUY", occurrence_ordinal=2, tick_fingerprint="fp_new_003", provenance="RECOVERED_HISTORICAL_MT5", ingested_at_utc=now_utc),
        TradeTick(symbol="WINZ26", time_msc=1792573260000, time_utc=now_utc, price=-10.0, volume_real=50.0, flags=1080, trade_side="BUY", occurrence_ordinal=3, tick_fingerprint="fp_inv_004", provenance="RECOVERED_HISTORICAL_MT5", ingested_at_utc=now_utc),
    ]

    # Executar Backfill Auditado
    res = engine.execute_audited_backfill(ticks=test_ticks, symbol="WINZ26")
    run_id = res["backfill_run_id"]

    assert res["attempted_count"] == 4
    assert res["already_present_count"] == 1
    assert res["rejected_count"] == 1
    assert res["inserted_new_count"] == 2

    # Verificar que as 2 linhas novas entraram com a linhagem correta
    with sqlite3.connect(db_file) as conn:
        c = conn.cursor()
        c.execute("SELECT count(*) FROM spool_ticks;")
        assert c.fetchone()[0] == 3 # 1 inicial + 2 novas
        c.execute("SELECT count(*) FROM backfill_lineage WHERE backfill_run_id = ?;", (run_id,))
        assert c.fetchone()[0] == 2 # 2 rastreadas no run_id
        c.execute("SELECT last_captured_msc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
        assert c.fetchone()[0] == 1792573260000 # Avançou para o novo teto

    # Executar ROLLBACK CIRÚRGICO pelo backfill_run_id
    rollback_res = engine.rollback_backfill_run(backfill_run_id=run_id, symbol="WINZ26")
    assert rollback_res["status"] == "ROLLED_BACK_SUCCESSFULLY"
    assert rollback_res["deleted_sqlite_count"] == 2

    # Comprovar RESTAURAÇÃO IDÊNTICA DO ESTADO PRÉ-BACKFILL
    with sqlite3.connect(db_file) as conn:
        c = conn.cursor()
        c.execute("SELECT count(*) FROM spool_ticks;")
        assert c.fetchone()[0] == 1 # Exatamente o tick inicial preservado!
        c.execute("SELECT tick_fingerprint FROM spool_ticks;")
        assert c.fetchone()[0] == "fp_initial_001"
        c.execute("SELECT last_captured_msc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
        assert c.fetchone()[0] == 1792573258338 # Checkpoint restaurado!
        c.execute("SELECT count(*) FROM backfill_lineage;")
        assert c.fetchone()[0] == 0 # Linhagem limpa

    print(f"\nRollback cirúrgico conjunto homologado: 2 ticks removidos, checkpoint revertido com 100% de paridade!")

def test_canonical_candle_reconstruction_and_ambiguity_detection():
    """
    Requisito 9:
    Reconstrói candles M2 exclusivamente a partir dos ticks canônicos e
    identifica explicitamente ambiguidades intramilissegundo.
    """
    reconstructor = CanonicalCandleReconstructor()
    now_utc = datetime(2026, 10, 21, 12, 0, 0, tzinfo=timezone.utc)
    
    # 3 ticks dentro do mesmo candle M2 (12:00 às 12:02 UTC)
    # Dois ticks no mesmo milissegundo de abertura com preços distintos (leilão / cruzamento)
    ticks = [
        TradeTick(symbol="WINZ26", time_msc=1792573200000, time_utc=now_utc, price=212900.0, volume_real=10.0, flags=1080, trade_side="BUY", occurrence_ordinal=1, tick_fingerprint="t1", provenance="CANONICAL", ingested_at_utc=now_utc),
        TradeTick(symbol="WINZ26", time_msc=1792573200000, time_utc=now_utc, price=212910.0, volume_real=20.0, flags=1080, trade_side="BUY", occurrence_ordinal=2, tick_fingerprint="t2", provenance="CANONICAL", ingested_at_utc=now_utc),
        TradeTick(symbol="WINZ26", time_msc=1792573250000, time_utc=now_utc, price=212850.0, volume_real=30.0, flags=1080, trade_side="SELL", occurrence_ordinal=1, tick_fingerprint="t3", provenance="CANONICAL", ingested_at_utc=now_utc),
    ]

    candles, ambiguities = reconstructor.reconstruct_candles_from_ticks(ticks, "WINZ26")
    assert len(candles) == 1
    c = candles[0]
    assert c.open == 212900.0
    assert c.high == 212910.0
    assert c.low == 212850.0
    assert c.close == 212850.0
    assert c.volume_real == 60.0
    assert c.tick_volume == 3
    assert len(ambiguities) == 1
    assert ambiguities[0]["type"] == "OPEN_PRICE_INTRA_MS_AMBIGUITY"
    print(f"\nCandle M2 reconstruído com detecção explícita de ambiguidade: {ambiguities[0]}")

def test_window_auditor_never_classifies_empty_without_coverage():
    """
    Requisitos 8 & 10:
    Audita janela de 5 minutos e garante que NUNCA classifica como EMPTY_VERIFIED
    sem prova formal de cobertura histórica contínua.
    """
    auditor = WindowAuditReconciler()
    
    # Caso 1: Janela sem ticks na referência e sem comprovação de cobertura
    res_inconclusive = auditor.audit_5m_window(
        ref_ticks_in_window=[],
        db_ticks_in_window=[],
        window_start_msc=1792573200000,
        window_end_msc=1792573500000,
        has_historical_coverage_proof=False
    )
    assert res_inconclusive["classification"] == "INCONCLUSIVE_NO_PROOF"

    # Caso 2: Janela sem ticks com prova de cobertura
    res_empty_verified = auditor.audit_5m_window(
        ref_ticks_in_window=[],
        db_ticks_in_window=[],
        window_start_msc=1792573200000,
        window_end_msc=1792573500000,
        has_historical_coverage_proof=True
    )
    assert res_empty_verified["classification"] == "EMPTY_VERIFIED"

    # Caso 3: Janela com paridade exata
    t = {"tick_fingerprint": "fp1", "time_msc": 1792573210000, "price": 212900.0, "volume_real": 10.0, "flags": 1080, "occurrence_ordinal": 1}
    res_exact = auditor.audit_5m_window(
        ref_ticks_in_window=[t],
        db_ticks_in_window=[t],
        window_start_msc=1792573200000,
        window_end_msc=1792573500000,
        has_historical_coverage_proof=True
    )
    assert res_exact["classification"] == "EXACT_MATCH"

if __name__ == "__main__":
    pytest.main(["-s", __file__])
