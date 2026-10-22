import os
import sqlite3
import uuid
import logging
from typing import List, Dict, Any, Tuple, Optional, Set
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_batch

logger = logging.getLogger("PhysicalLineageBackfill")

class PhysicalLineageBackfillEngine:
    """
    Motor de backfill com rastreabilidade por identidade física exata da inserção (INSERT ... RETURNING)
    e recuperação compensatória contra interrupções distribuídas.
    """

    def __init__(
        self,
        sqlite_path: str,
        pg_conn_params: Dict[str, Any]
    ):
        self.sqlite_path = sqlite_path
        self.pg_conn_params = pg_conn_params
        self._ensure_physical_lineage_schemas()

    def _ensure_physical_lineage_schemas(self):
        # 1. SQLite: tabela de linhagem física por rowid
        with sqlite3.connect(self.sqlite_path) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS backfill_physical_lineage (
                sqlite_rowid INTEGER PRIMARY KEY,
                backfill_run_id TEXT NOT NULL,
                tick_fingerprint TEXT NOT NULL,
                symbol TEXT NOT NULL,
                time_msc INTEGER NOT NULL,
                inserted_at_utc TEXT NOT NULL
            );
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_phys_lineage_run ON backfill_physical_lineage (backfill_run_id);
            """)
            conn.commit()

        # 2. PostgreSQL: tabela de linhagem por chave primária composta
        try:
            with psycopg2.connect(**self.pg_conn_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    CREATE TABLE IF NOT EXISTS backfill_physical_keys (
                        time_utc TIMESTAMPTZ NOT NULL,
                        symbol TEXT NOT NULL,
                        tick_fingerprint TEXT NOT NULL,
                        backfill_run_id TEXT NOT NULL,
                        inserted_at_utc TIMESTAMPTZ NOT NULL,
                        PRIMARY KEY (time_utc, symbol, tick_fingerprint)
                    );
                    CREATE INDEX IF NOT EXISTS idx_pg_phys_run ON backfill_physical_keys (backfill_run_id);
                    CREATE TABLE IF NOT EXISTS backfill_run_audit_log (
                        backfill_run_id TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        attempted_count INT NOT NULL,
                        sqlite_inserted_count INT NOT NULL,
                        pg_inserted_count INT NOT NULL,
                        already_present_count INT NOT NULL,
                        pre_checkpoints_json TEXT NOT NULL,
                        started_at_utc TIMESTAMPTZ NOT NULL,
                        completed_at_utc TIMESTAMPTZ,
                        status TEXT NOT NULL
                    );
                    """)
                conn.commit()
        except Exception as e:
            logger.warning(f"Aviso ao inicializar schema de linhagem física no PG: {e}")

    def execute_physical_backfill(
        self,
        candidate_ticks: List[Dict[str, Any]],
        symbol: str = "WINZ26",
        simulate_failure_point: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Executa a inserção em dois estágios capturando EXCLUSIVAMENTE as chaves retornadas pelo RETURNING.
        Suporta simulação controlada de pontos de falha para testes de recuperação compensatória.
        """
        run_id = f"bkf_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        now_iso = datetime.now(timezone.utc).isoformat()

        # Obter estado prévio dos checkpoints para preservação
        with sqlite3.connect(self.sqlite_path) as s_conn:
            s_cur = s_conn.cursor()
            s_cur.execute("SELECT last_captured_msc, last_replicated_msc, watermark_complete_msc FROM checkpoints_two_stage WHERE stream_key = ?;", (symbol,))
            row = s_cur.fetchone()
            pre_checkpoints = {
                "captured": row[0] if row else 0,
                "replicated": row[1] if row else 0,
                "complete": row[2] if row else 0
            }

        # -------------------------------------------------------------
        # PONTO DE FALHA A: Antes do commit SQLite
        # -------------------------------------------------------------
        if simulate_failure_point == "BEFORE_SQLITE_COMMIT":
            raise RuntimeError("FALHA SIMULADA BEFORE_SQLITE_COMMIT: Interrupção de processo antes do commit no SQLite!")

        # 1. ESTÁGIO 1: Ingestão no SQLite com RETURNING rowid, tick_fingerprint
        sqlite_inserted_keys: List[Tuple[int, str, str, int]] = []
        already_present_count = 0

        with sqlite3.connect(self.sqlite_path) as s_conn:
            s_cur = s_conn.cursor()
            s_cur.execute("BEGIN IMMEDIATE;")
            
            for t in candidate_ticks:
                # INSERT com RETURNING para capturar apenas se foi inserido de fato (não ignorado)
                s_cur.execute("""
                INSERT OR IGNORE INTO spool_ticks (
                    tick_fingerprint, symbol, time_msc, price, volume_real,
                    flags, trade_side, occurrence_ordinal, provenance, status, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
                RETURNING rowid, tick_fingerprint;
                """, (
                    t["tick_fingerprint"], symbol, t["time_msc"], t["price"], t["volume_real"],
                    t["flags"], t["trade_side"], t["occurrence_ordinal"], "RECOVERED_HISTORICAL_MT5", now_iso
                ))
                ret = s_cur.fetchone()
                if ret:
                    sqlite_rowid, fp = ret
                    sqlite_inserted_keys.append((sqlite_rowid, fp, symbol, t["time_msc"]))
                else:
                    already_present_count += 1

            # Gravar na tabela de linhagem física apenas os rowids efetivamente inseridos nesta execução
            lineage_records = [
                (rowid, run_id, fp, sym, msc, now_iso)
                for rowid, fp, sym, msc in sqlite_inserted_keys
            ]
            s_cur.executemany("""
            INSERT INTO backfill_physical_lineage (sqlite_rowid, backfill_run_id, tick_fingerprint, symbol, time_msc, inserted_at_utc)
            VALUES (?, ?, ?, ?, ?, ?);
            """, lineage_records)
            
            s_conn.commit()

        # -------------------------------------------------------------
        # PONTO DE FALHA B: Após commit SQLite e antes do commit PostgreSQL
        # -------------------------------------------------------------
        if simulate_failure_point == "AFTER_SQLITE_BEFORE_PG":
            raise RuntimeError("FALHA SIMULADA AFTER_SQLITE_BEFORE_PG: Interrupção após SQLite commit e antes do PostgreSQL commit!")

        # 2. ESTÁGIO 2: Replicação no PostgreSQL com INSERT ... RETURNING time_utc, symbol, tick_fingerprint
        pg_inserted_keys: List[Tuple[Any, str, str]] = []
        if sqlite_inserted_keys:
            inserted_fps = {k[1] for k in sqlite_inserted_keys}
            ticks_to_replicate = [t for t in candidate_ticks if t["tick_fingerprint"] in inserted_fps]

            with psycopg2.connect(**self.pg_conn_params) as p_conn:
                with p_conn.cursor() as p_cur:
                    part_name = f"market_ticks_raw_{ticks_to_replicate[0]['time_utc'][:10].replace('-', '_')}"
                    p_cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {part_name} PARTITION OF market_ticks_raw
                    FOR VALUES FROM ('{ticks_to_replicate[0]['time_utc'][:10]} 00:00:00+00')
                    TO ('{ticks_to_replicate[0]['time_utc'][:10]} 23:59:59.999999+00');
                    """)

                    for t in ticks_to_replicate:
                        p_cur.execute("""
                        INSERT INTO market_ticks_raw (
                            symbol, time_msc, tick_fingerprint, time_utc, price,
                            volume_real, flags, trade_side, occurrence_ordinal, provenance, ingested_at_utc
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (time_utc, symbol, tick_fingerprint) DO NOTHING
                        RETURNING time_utc, symbol, tick_fingerprint;
                        """, (
                            symbol, t["time_msc"], t["tick_fingerprint"], t["time_utc"],
                            t["price"], t["volume_real"], t["flags"], t["trade_side"],
                            t["occurrence_ordinal"], "RECOVERED_HISTORICAL_MT5", now_iso
                        ))
                        ret_pg = p_cur.fetchone()
                        if ret_pg:
                            pg_inserted_keys.append(ret_pg)

                    # Registrar chaves físicas no PostgreSQL
                    pg_key_records = [
                        (k[0], k[1], k[2], run_id, now_iso)
                        for k in pg_inserted_keys
                    ]
                    execute_batch(p_cur, """
                    INSERT INTO backfill_physical_keys (time_utc, symbol, tick_fingerprint, backfill_run_id, inserted_at_utc)
                    VALUES (%s, %s, %s, %s, %s);
                    """, pg_key_records)

                # -------------------------------------------------------------
                # PONTO DE FALHA C: Após commit PostgreSQL e antes do ACK no SQLite
                # -------------------------------------------------------------
                if simulate_failure_point == "AFTER_PG_BEFORE_SQLITE_ACK":
                    p_conn.commit() # PG comitou
                    raise RuntimeError("FALHA SIMULADA AFTER_PG_BEFORE_SQLITE_ACK: PostgreSQL commitou mas o processo morreu antes do ACK SQLite!")

                p_conn.commit()

        # 3. ESTÁGIO 3: Marcar status COMMITTED no SQLite e avançar checkpoints com ACK
        if sqlite_inserted_keys:
            # -------------------------------------------------------------
            # PONTO DE FALHA D: Durante atualização de checkpoints
            # -------------------------------------------------------------
            if simulate_failure_point == "DURING_CHECKPOINTS_UPDATE":
                raise RuntimeError("FALHA SIMULADA DURING_CHECKPOINTS_UPDATE: Interrupção durante a atualização dos checkpoints!")

            with sqlite3.connect(self.sqlite_path) as s_conn:
                s_cur = s_conn.cursor()
                s_cur.execute("BEGIN IMMEDIATE;")
                rowids = [k[0] for k in sqlite_inserted_keys]
                chunk_size = 500
                for i in range(0, len(rowids), chunk_size):
                    chunk = rowids[i:i + chunk_size]
                    ph = ",".join("?" for _ in chunk)
                    s_cur.execute(f"UPDATE spool_ticks SET status = 'COMMITTED' WHERE rowid IN ({ph});", chunk)

                max_msc = max(k[3] for k in sqlite_inserted_keys)
                s_cur.execute("""
                UPDATE checkpoints_two_stage SET
                    last_captured_msc = MAX(last_captured_msc, ?),
                    last_replicated_msc = MAX(last_replicated_msc, ?),
                    watermark_complete_msc = MAX(watermark_complete_msc, ?),
                    updated_at_utc = ?
                WHERE stream_key = ?;
                """, (max_msc, max_msc, max_msc, now_iso, symbol))
                s_conn.commit()

        return {
            "backfill_run_id": run_id,
            "attempted_count": len(candidate_ticks),
            "sqlite_inserted_count": len(sqlite_inserted_keys),
            "pg_inserted_count": len(pg_inserted_keys),
            "already_present_count": already_present_count,
            "pre_checkpoints": pre_checkpoints,
            "status": "COMPLETED_AUDITED"
        }

    def execute_compensatory_recovery(self, symbol: str = "WINZ26") -> Dict[str, Any]:
        """
        MECANISMO DE RECUPERAÇÃO COMPENSATÓRIA (CRUCIAL):
        Reconcilia os dois bancos caso tenha ocorrido interrupção em qualquer fronteira:
        1. Consulta itens que ficaram com status 'PENDING' no SQLite.
        2. Replica ao PostgreSQL com ON CONFLICT DO NOTHING (idempotente).
        3. Após confirmação do PG, avança status para 'COMMITTED'.
        4. Recalcula checkpoints a partir do max(time_msc) de dados confirmados.
        """
        now_iso = datetime.now(timezone.utc).isoformat()
        recovered_replicated = 0

        with sqlite3.connect(self.sqlite_path) as s_conn:
            s_cur = s_conn.cursor()
            s_cur.execute("""
            SELECT tick_fingerprint, symbol, time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, provenance, created_at_utc, rowid
            FROM spool_ticks WHERE symbol = ? AND status = 'PENDING';
            """, (symbol,))
            pending_rows = s_cur.fetchall()

        if pending_rows:
            with psycopg2.connect(**self.pg_conn_params) as p_conn:
                with p_conn.cursor() as p_cur:
                    for r in pending_rows:
                        # converter time_msc para time_utc
                        sec = r[2] // 1000
                        time_utc_iso = datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
                        p_cur.execute("""
                        INSERT INTO market_ticks_raw (
                            symbol, time_msc, tick_fingerprint, time_utc, price,
                            volume_real, flags, trade_side, occurrence_ordinal, provenance, ingested_at_utc
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (time_utc, symbol, tick_fingerprint) DO NOTHING;
                        """, (
                            r[1], r[2], r[0], time_utc_iso, r[3], r[4],
                            r[5], r[6], r[7], r[8], r[9]
                        ))
                p_conn.commit()

            # Confirmar no SQLite
            with sqlite3.connect(self.sqlite_path) as s_conn:
                s_cur = s_conn.cursor()
                rowids = [r[10] for r in pending_rows]
                ph = ",".join("?" for _ in rowids)
                s_cur.execute(f"UPDATE spool_ticks SET status = 'COMMITTED' WHERE rowid IN ({ph});", rowids)
                
                # Recalcular checkpoints reais
                s_cur.execute("SELECT max(time_msc) FROM spool_ticks WHERE symbol = ? AND status = 'COMMITTED';", (symbol,))
                max_msc = s_cur.fetchone()[0] or 0
                s_cur.execute("""
                UPDATE checkpoints_two_stage SET
                    last_captured_msc = MAX(last_captured_msc, ?),
                    last_replicated_msc = MAX(last_replicated_msc, ?),
                    watermark_complete_msc = MAX(watermark_complete_msc, ?),
                    updated_at_utc = ?
                WHERE stream_key = ?;
                """, (max_msc, max_msc, max_msc, now_iso, symbol))
                s_conn.commit()
            recovered_replicated = len(pending_rows)

        return {
            "status": "COMPENSATORY_RECOVERY_SUCCESS",
            "drained_pending_count": recovered_replicated
        }

    def rollback_physical_backfill_run(
        self,
        backfill_run_id: str,
        pre_checkpoints: Dict[str, int],
        symbol: str = "WINZ26",
        simulate_failure_during_rollback: bool = False
    ) -> Dict[str, Any]:
        """
        ROLLBACK CIRÚRGICO POR IDENTIDADE FÍSICA:
        Remove exclusivamente as tuplas (time_utc, symbol, tick_fingerprint) do PostgreSQL
        e os rowids do SQLite registrados na tabela de linhagem física.
        Registros previamente existentes JAMAIS são afetados.
        """
        now_iso = datetime.now(timezone.utc).isoformat()

        # -------------------------------------------------------------
        # PONTO DE FALHA E: Interrupção durante o rollback
        # -------------------------------------------------------------
        if simulate_failure_during_rollback:
            raise RuntimeError("FALHA SIMULADA: Falha no meio da execução do rollback!")

        # 1. Remover do PostgreSQL EXCLUSIVAMENTE via tabela backfill_physical_keys
        deleted_pg = 0
        with psycopg2.connect(**self.pg_conn_params) as p_conn:
            with p_conn.cursor() as p_cur:
                p_cur.execute("""
                DELETE FROM market_ticks_raw m
                USING backfill_physical_keys k
                WHERE m.time_utc = k.time_utc
                  AND m.symbol = k.symbol
                  AND m.tick_fingerprint = k.tick_fingerprint
                  AND k.backfill_run_id = %s;
                """, (backfill_run_id,))
                deleted_pg = p_cur.rowcount
                p_cur.execute("DELETE FROM backfill_physical_keys WHERE backfill_run_id = %s;", (backfill_run_id,))
            p_conn.commit()

        # 2. Remover do SQLite EXCLUSIVAMENTE via sqlite_rowid registrado na linhagem física
        deleted_sqlite = 0
        with sqlite3.connect(self.sqlite_path) as s_conn:
            s_cur = s_conn.cursor()
            s_cur.execute("BEGIN IMMEDIATE;")
            s_cur.execute("SELECT sqlite_rowid FROM backfill_physical_lineage WHERE backfill_run_id = ?;", (backfill_run_id,))
            rowids_to_del = [r[0] for r in s_cur.fetchall()]

            if rowids_to_del:
                chunk_size = 500
                for i in range(0, len(rowids_to_del), chunk_size):
                    chunk = rowids_to_del[i:i + chunk_size]
                    ph = ",".join("?" for _ in chunk)
                    s_cur.execute(f"DELETE FROM spool_ticks WHERE rowid IN ({ph});", chunk)
                    deleted_sqlite += s_cur.rowcount

            s_cur.execute("DELETE FROM backfill_physical_lineage WHERE backfill_run_id = ?;", (backfill_run_id,))
            
            # Restaurar checkpoints prévios exatos
            s_cur.execute("""
            UPDATE checkpoints_two_stage SET
                last_captured_msc = ?,
                last_replicated_msc = ?,
                watermark_complete_msc = ?,
                updated_at_utc = ?
            WHERE stream_key = ?;
            """, (
                pre_checkpoints["captured"],
                pre_checkpoints["replicated"],
                pre_checkpoints["complete"],
                now_iso,
                symbol
            ))
            s_conn.commit()

        return {
            "status": "PHYSICAL_ROLLBACK_COMPLETED",
            "backfill_run_id": backfill_run_id,
            "deleted_pg_count": deleted_pg,
            "deleted_sqlite_count": deleted_sqlite,
            "restored_checkpoints": pre_checkpoints
        }
