import os
import sys
import time
import psycopg2
from datetime import datetime, timezone, timedelta

BRT = timezone(timedelta(hours=-3))

def main():
    print("=" * 80)
    print("HARDENING H001 — ETAPA B: MIGRAÇÃO HISTÓRICA SUPERVISIONADA (PORTA 5434)")
    print(f"Início: {datetime.now(BRT).strftime('%Y-%m-%d %H:%M:%S BRT')}")
    print("=" * 80)

    # Conectar ao PostgreSQL operacional na porta 5434
    db_params = {
        "host": "127.0.0.1",
        "port": 5434,
        "dbname": "market_data",
        "user": "postgres",
        "password": "password"
    }

    migration_dir = "c:/Users/fie23/Projetos/win-market-monitor/sql/migrations/h001"
    scripts = [
        "01_precondition_check.sql",
        "02_schema_evolution.sql",
        "03_reclassify_provenance_nc001.sql",
        "04_mark_duplicate_transport_artefacts_nc003.sql",
        "05_insert_missing_historical_ticks_nc003.sql",
        "06_create_canonical_views.sql",
        "07_rebuild_candles_m2_v2.sql",
        "08_postcondition_check.sql"
    ]

    conn = psycopg2.connect(**db_params)
    conn.autocommit = False  # Controle transacional rigoroso

    execution_log = []

    try:
        with conn.cursor() as cur:
            for s_idx, s_name in enumerate(scripts, start=1):
                s_path = os.path.join(migration_dir, s_name)
                print(f"\n[{s_idx}/{len(scripts)}] Executando {s_name}...")
                t_start = time.time()

                with open(s_path, "r", encoding="utf-8") as f:
                    sql_content = f.read()

                cur.execute(sql_content)
                elapsed = time.time() - t_start
                print(f"  -> Concluído em {elapsed:.2f}s com sucesso!")
                execution_log.append({
                    "step": s_idx,
                    "script": s_name,
                    "elapsed_sec": elapsed,
                    "status": "SUCCESS"
                })

        # Commit da transação global
        print("\nCommitando transações de migração no PostgreSQL operacional...")
        conn.commit()
        print("  -> Transação confirmada com sucesso (COMMIT OK).")

    except Exception as e:
        print(f"\nERRO CRÍTICO NA MIGRAÇÃO: {e}")
        print("Executando ROLLBACK da transação...")
        conn.rollback()
        conn.close()
        sys.exit(1)

    # Verificações e Auditoria Detalhada Pós-Migração
    print("\n" + "=" * 80)
    print("AUDITORIA PÓS-MIGRAÇÃO HISTÓRICA NO BANCO OPERACIONAL")
    print("=" * 80)

    conn_audit = psycopg2.connect(**db_params)
    with conn_audit.cursor() as cur:
        # 1. Total bruto em market_ticks_raw_2026_10_19
        cur.execute("SELECT COUNT(*), SUM(volume_real) FROM market_ticks_raw_2026_10_19;")
        raw_ticks, raw_vol = cur.fetchone()
        print(f"Total Bruto Físico (market_ticks_raw 19/10): {raw_ticks} ticks | Volume: {raw_vol}")
        # Esperado: 2.093.221 + 511 = 2.093.732 ticks brutos (preservando brutos + injetados)

        # 2. Total canônico
        cur.execute("SELECT COUNT(*), SUM(volume_real) FROM v_market_ticks_canonical WHERE symbol = 'WINZ26';")
        canon_ticks, canon_vol = cur.fetchone()
        print(f"Total Canônico (v_market_ticks_canonical): {canon_ticks} ticks | Volume: {canon_vol}")

        # 3. Linhagem de duplicatas
        cur.execute("SELECT COUNT(*), SUM(duplicate_volume_real) FROM duplicate_lineage;")
        dup_lineage_cnt, dup_lineage_vol = cur.fetchone()
        print(f"Linhagem de Duplicatas (duplicate_lineage): {dup_lineage_cnt} registros | Volume: {dup_lineage_vol}")

        # 4. Injeções de ausentes
        cur.execute("SELECT COUNT(*) FROM h001_injected_ticks;")
        inj_cnt = cur.fetchone()[0]
        print(f"Negócios Ausentes Injetados (h001_injected_ticks): {inj_cnt} ticks")

        # 5. Reclassificação NC-001
        cur.execute("SELECT COUNT(*) FROM h001_reclassified_nc001_ticks;")
        reclass_cnt = cur.fetchone()[0]
        print(f"Reclassificados NC-001 (h001_reclassified_nc001_ticks): {reclass_cnt} ticks")

        # 6. Candles M2 v2
        cur.execute("SELECT COUNT(*), SUM(volume_real) FROM candles_m2_v2 WHERE symbol = 'WINZ26';")
        candles_v2_cnt, candles_v2_vol = cur.fetchone()
        print(f"Candles M2 v2: {candles_v2_cnt} candles | Volume: {candles_v2_vol}")

        # Asserções finais de integridade
        assert canon_ticks == 2093566, f"Canônico divergiu: {canon_ticks} != 2093566"
        assert canon_vol == 19816479, f"Volume canônico divergiu: {canon_vol} != 19816479"
        assert dup_lineage_cnt == 166, f"Linhagem divergiu: {dup_lineage_cnt} != 166"
        assert dup_lineage_vol == 26070, f"Volume duplicatas divergiu: {dup_lineage_vol} != 26070"
        assert inj_cnt == 511, f"Injeções divergiram: {inj_cnt} != 511"
        assert reclass_cnt == 44899, f"Reclassificados divergiram: {reclass_cnt} != 44899"
        assert candles_v2_cnt == 284, f"Candles divergiram: {candles_v2_cnt} != 284"
        assert candles_v2_vol == 19816479, f"Volume candles divergiu: {candles_v2_vol} != 19816479"

    conn_audit.close()

    print("\n" + "=" * 80)
    print("ETAPA B CONCLUÍDA COM 100% DE SUCESSO NO POSTGRESQL OPERACIONAL!")
    print(f"Fim: {datetime.now(BRT).strftime('%Y-%m-%d %H:%M:%S BRT')}")
    print("=" * 80)

if __name__ == "__main__":
    main()
