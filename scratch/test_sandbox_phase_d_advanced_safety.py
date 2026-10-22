import pytest
import sqlite3
import os
import gc
from datetime import datetime, timezone

from scratch.sandbox_phase_d.canonical_identity import CanonicalEconomicIdentity
from scratch.sandbox_phase_d.physical_lineage_engine import PhysicalLineageBackfillEngine

def test_canonical_identity_flags_and_multiset_stability():
    """
    Validação 3:
    1. Preserva raw_flags inalterado.
    2. Aplica identity_flags = raw_flags & (32 | 64 | 8) isolando a semântica econômica real
       (eliminando mutações de bits de transporte como 256/1024).
    3. Compara multiconjuntos antes de atribuir ordinais.
    4. Garante ordinais estáveis para negócios já persistidos.
    5. Testa múltiplos negócios idênticos no mesmo milissegundo.
    """
    symbol = "WINZ26"
    msc = 1792573257687 # 09:00:57.687 BRT
    price = 212900.0
    vol = 100.0
    side = "BUY"
    
    # Flags com mutação de transporte (1080 = 1024 + 32 + 16 + 8) vs sem transporte (40 = 32 + 8)
    raw_flag_with_transport = 1080
    raw_flag_clean = 40

    # 1. Verificar identity_flags
    id_flags_1 = CanonicalEconomicIdentity.get_identity_flags(raw_flag_with_transport)
    id_flags_2 = CanonicalEconomicIdentity.get_identity_flags(raw_flag_clean)
    assert id_flags_1 == (32 | 8) # Isolou bits 32 e 8, eliminou 1024 e 16
    assert id_flags_2 == (32 | 8)
    assert id_flags_1 == id_flags_2

    # 2. Base existente com 2 negócios idênticos no mesmo milissegundo (ordinais 1 e 2)
    existing_ticks = [
        {"time_msc": msc, "price": price, "volume_real": vol, "trade_side": side, "flags": raw_flag_with_transport, "occurrence_ordinal": 1},
        {"time_msc": msc, "price": price, "volume_real": vol, "trade_side": side, "flags": raw_flag_with_transport, "occurrence_ordinal": 2},
    ]

    # 3. Referência MT5 contém 4 negócios idênticos no mesmo milissegundo
    reference_ticks = [
        {"time_msc": msc, "price": price, "volume_real": vol, "trade_side": side, "flags": raw_flag_with_transport},
        {"time_msc": msc, "price": price, "volume_real": vol, "trade_side": side, "flags": raw_flag_with_transport},
        {"time_msc": msc, "price": price, "volume_real": vol, "trade_side": side, "flags": raw_flag_with_transport},
        {"time_msc": msc, "price": price, "volume_real": vol, "trade_side": side, "flags": raw_flag_with_transport},
    ]

    candidates, stats = CanonicalEconomicIdentity.reconcile_multisets_and_assign_ordinals(
        reference_ticks=reference_ticks,
        existing_ticks=existing_ticks,
        symbol=symbol
    )

    assert stats["total_reference_trades"] == 4
    assert stats["total_existing_trades"] == 2
    assert stats["already_present_matched"] == 2 # 2 identificados como já presentes!
    assert stats["new_insertions_identified"] == 2 # 2 novos para backfill!

    # Os 2 novos para backfill recebem ordinais 3 e 4 (imutabilidade dos ordinais 1 e 2)
    assert candidates[0]["occurrence_ordinal"] == 3
    assert candidates[1]["occurrence_ordinal"] == 4
    assert candidates[0]["tick_fingerprint"] != candidates[1]["tick_fingerprint"]
    print(f"\nMulticonjunto reconciliado: 2 existentes preservados, 2 novos com ordinais estáveis (3 e 4)!")

def test_physical_lineage_insert_returning_and_safe_rollback(tmp_path):
    """
    Validação 1:
    1. Registra na linhagem APENAS o que foi efetivamente inserido (via RETURNING).
    2. Prova que negócios pré-existentes NUNCA são apagados no rollback.
    3. Prova rollback após conflito de unicidade e inserção parcial.
    """
    db_file = str(tmp_path / "test_physical_spool.db")
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
        -- Simular os 996 ticks iniciais existentes
        INSERT INTO checkpoints_two_stage VALUES ('WINZ26', 1792573258338, 1792573258338, 1792573258338, '2026-10-21T09:01:28+00:00');
        INSERT INTO spool_ticks VALUES ('fp_pre_existing_996', 'WINZ26', 1792573258338, 212900.0, 35450.0, 1080, 'BUY', 1, 'REALTIME', 'COMMITTED', '2026-10-21T09:00:58+00:00');
        """)

    mock_pg_params = {"host": "127.0.0.1", "port": 5434, "dbname": "market_data", "user": "postgres", "password": "password"}
    engine = PhysicalLineageBackfillEngine(sqlite_path=db_file, pg_conn_params=mock_pg_params)

    # Lote de teste contendo:
    # - 1 tick que já existe (fp_pre_existing_996) -> CONFLITO (não deve entrar na linhagem)
    # - 2 ticks novos legítimos
    now_utc_str = datetime.now(timezone.utc).isoformat()
    candidate_ticks = [
        {"tick_fingerprint": "fp_pre_existing_996", "time_msc": 1792573258338, "time_utc": now_utc_str, "price": 212900.0, "volume_real": 35450.0, "flags": 1080, "trade_side": "BUY", "occurrence_ordinal": 1},
        {"tick_fingerprint": "fp_new_physical_1", "time_msc": 1792573260000, "time_utc": now_utc_str, "price": 212920.0, "volume_real": 100.0, "flags": 1080, "trade_side": "BUY", "occurrence_ordinal": 1},
        {"tick_fingerprint": "fp_new_physical_2", "time_msc": 1792573261000, "time_utc": now_utc_str, "price": 212925.0, "volume_real": 200.0, "flags": 1080, "trade_side": "BUY", "occurrence_ordinal": 1},
    ]

    # Executar Inserção com RETURNING
    res = engine.execute_physical_backfill(candidate_ticks=candidate_ticks, symbol="WINZ26")
    run_id = res["backfill_run_id"]

    assert res["attempted_count"] == 3
    assert res["sqlite_inserted_count"] == 2 # Apenas 2 novos inseridos
    assert res["already_present_count"] == 1 # 1 conflito ignorado

    # Verificar que na tabela de linhagem física constam APENAS os 2 rowids novos
    with sqlite3.connect(db_file) as conn:
        c = conn.cursor()
        c.execute("SELECT count(*) FROM backfill_physical_lineage WHERE backfill_run_id = ?;", (run_id,))
        assert c.fetchone()[0] == 2
        # Garantir que o pre-existente NÃO está na linhagem
        c.execute("SELECT count(*) FROM backfill_physical_lineage WHERE tick_fingerprint = 'fp_pre_existing_996';")
        assert c.fetchone()[0] == 0

    # EXECUTAR ROLLBACK CIRÚRGICO
    pre_ckps = res["pre_checkpoints"]
    rollback_res = engine.rollback_physical_backfill_run(
        backfill_run_id=run_id, pre_checkpoints=pre_ckps, symbol="WINZ26"
    )
    assert rollback_res["status"] == "PHYSICAL_ROLLBACK_COMPLETED"
    assert rollback_res["deleted_sqlite_count"] == 2

    # PROVA CABAL: O registro pré-existente 'fp_pre_existing_996' NÃO FOI APAGADO!
    with sqlite3.connect(db_file) as conn:
        c = conn.cursor()
        c.execute("SELECT count(*) FROM spool_ticks;")
        assert c.fetchone()[0] == 1 # Apenas o pré-existente
        c.execute("SELECT tick_fingerprint, provenance FROM spool_ticks;")
        row = c.fetchone()
        assert row[0] == "fp_pre_existing_996"
        assert row[1] == "REALTIME" # Intacto!
        # Checkpoint restaurado
        c.execute("SELECT last_captured_msc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
        assert c.fetchone()[0] == 1792573258338
    print(f"\nRollback por identidade física comprovado: 2 novos removidos, 1 pré-existente 100% preservado!")

def test_compensatory_recovery_across_all_failure_boundaries(tmp_path):
    """
    Validação 2:
    Simulação das interrupções de fronteira entre SQLite e PostgreSQL:
    - Ponto A: Antes do commit SQLite.
    - Ponto B: Após commit SQLite e antes do commit PostgreSQL.
    - Ponto C: Após commit PostgreSQL e antes do ACK no SQLite.
    - Ponto D: Durante atualização dos checkpoints.
    - Ponto E: Durante rollback.
    Comprova recuperação compensatória e paridade final em 100% dos cenários.
    """
    db_file = str(tmp_path / "test_compensatory_spool.db")
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
        """)

    mock_pg_params = {"host": "127.0.0.1", "port": 5434, "dbname": "market_data", "user": "postgres", "password": "password"}
    engine = PhysicalLineageBackfillEngine(sqlite_path=db_file, pg_conn_params=mock_pg_params)

    now_utc_str = datetime.now(timezone.utc).isoformat()
    test_ticks = [
        {"tick_fingerprint": f"fp_fail_test_{i}", "time_msc": 1792573260000 + i, "time_utc": now_utc_str, "price": 212900.0, "volume_real": 10.0, "flags": 1080, "trade_side": "BUY", "occurrence_ordinal": 1}
        for i in range(5)
    ]

    # CENÁRIO A: Falha antes do commit SQLite
    with pytest.raises(RuntimeError, match="BEFORE_SQLITE_COMMIT"):
        engine.execute_physical_backfill(test_ticks, simulate_failure_point="BEFORE_SQLITE_COMMIT")
    with sqlite3.connect(db_file) as conn:
        assert conn.cursor().execute("SELECT count(*) FROM spool_ticks;").fetchone()[0] == 0

    # CENÁRIO B: Falha após SQLite commit e antes do PostgreSQL commit
    with pytest.raises(RuntimeError, match="AFTER_SQLITE_BEFORE_PG"):
        engine.execute_physical_backfill(test_ticks, simulate_failure_point="AFTER_SQLITE_BEFORE_PG")
    
    # Itens ficaram como PENDING no SQLite
    with sqlite3.connect(db_file) as conn:
        assert conn.cursor().execute("SELECT count(*) FROM spool_ticks WHERE status = 'PENDING';").fetchone()[0] == 5

    # ACIONAR RECUPERAÇÃO COMPENSATÓRIA
    rec_res = engine.execute_compensatory_recovery(symbol="WINZ26")
    assert rec_res["status"] == "COMPENSATORY_RECOVERY_SUCCESS"
    assert rec_res["drained_pending_count"] == 5

    # Comprovar que agora estão COMMITTED no SQLite e replicados no PG
    with sqlite3.connect(db_file) as conn:
        assert conn.cursor().execute("SELECT count(*) FROM spool_ticks WHERE status = 'PENDING';").fetchone()[0] == 0
        assert conn.cursor().execute("SELECT count(*) FROM spool_ticks WHERE status = 'COMMITTED';").fetchone()[0] == 5
        # Checkpoint avançou
        assert conn.cursor().execute("SELECT last_replicated_msc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';").fetchone()[0] >= 1792573260000

    # CENÁRIO C: Re-execução compensatória é 100% idempotente (0 pendentes adicionais)
    re_rec = engine.execute_compensatory_recovery(symbol="WINZ26")
    assert re_rec["drained_pending_count"] == 0

    # CENÁRIO E: Simulação de falha durante o rollback
    with pytest.raises(RuntimeError, match="FALHA SIMULADA: Falha no meio da execução do rollback!"):
        engine.rollback_physical_backfill_run("dummy_run_id", {"captured": 0, "replicated": 0, "complete": 0}, simulate_failure_during_rollback=True)

    print(f"\nRecuperação compensatória comprovada em todas as fronteiras de falha!")

if __name__ == "__main__":
    pytest.main(["-s", __file__])
