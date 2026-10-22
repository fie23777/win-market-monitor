import os
import sys
import hashlib
import psycopg2
import sqlite3
import subprocess
from datetime import datetime, timezone, timedelta

BRT = timezone(timedelta(hours=-3))

def get_pg_connection(port=5434, dbname="market_data", read_only=True):
    user = "postgres" if port == 5434 else "admin"
    password = "password" if port == 5434 else "admin_password"
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        options="-c default_transaction_read_only=on" if read_only else ""
    )
    return conn


def compute_sha256(filepath):
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            sha.update(chunk)
    return sha.hexdigest()

def main():
    print("=" * 80)
    print("HARDENING H001 — ETAPA A: PRÉ-IMPLANTAÇÃO E VALIDAÇÃO DE PRÉ-CONDIÇÕES")
    print(f"Data/Hora: {datetime.now(BRT).strftime('%Y-%m-%d %H:%M:%S BRT')}")
    print("=" * 80)

    # 1. Checar conexões ativas na porta 5434
    print("\n[1/6] Verificando conexões ativas e ausência de escritas concorrentes no PostgreSQL (5434)...")
    conn_prod = get_pg_connection(port=5434, dbname="market_data", read_only=True)
    with conn_prod.cursor() as cur:
        cur.execute("""
            SELECT pid, usename, client_addr, state, query
            FROM pg_stat_activity
            WHERE datname = 'market_data' AND pid <> pg_backend_pid();
        """)
        active_sessions = cur.fetchall()
        print(f"Sessões concorrentes encontradas: {len(active_sessions)}")
        if active_sessions:
            for s in active_sessions:
                print(f"  PID: {s[0]}, User: {s[1]}, Addr: {s[2]}, State: {s[3]}, Query: {s[4][:50]}")
            print("ERRO: Existem sessões ativas no banco operacional!")
            sys.exit(1)
        print("  -> Nenhuma sessão concorrente ativa. Ausência de escrita confirmada.")

        # 2. Pré-condições do PostgreSQL Operacional
        print("\n[2/6] Verificando métricas e pré-condições da base operacional (5434)...")
        cur.execute("""
            SELECT COUNT(*), SUM(volume_real)
            FROM market_ticks_raw_2026_10_19
            WHERE symbol = 'WINZ26';
        """)
        ticks_prod, vol_prod = cur.fetchone()
        print(f"  market_ticks_raw_2026_10_19 total: {ticks_prod} ticks (esperado: 2093221)")
        print(f"  market_ticks_raw_2026_10_19 volume: {vol_prod} contratos (esperado: 19838713)")

        cur.execute("""
            SELECT COUNT(*)
            FROM market_ticks_raw_2026_10_19
            WHERE provenance = 'RECOVERED_HISTORICAL_MT5';
        """)
        backfill_cnt = cur.fetchone()[0]
        print(f"  RECOVERED_HISTORICAL_MT5 (NC-001): {backfill_cnt} ticks (esperado: 759845)")

        cur.execute("SELECT COUNT(*) FROM candles_m2 WHERE symbol = 'WINZ26';")
        candles_cnt = cur.fetchone()[0]
        print(f"  candles_m2 total: {candles_cnt} candles (esperado: 284)")

        cur.execute("SELECT * FROM collector_checkpoints;")
        ckp_rows = cur.fetchall()
        print(f"  collector_checkpoints PostgreSQL: {ckp_rows}")

        if ticks_prod != 2093221 or vol_prod != 19838713 or backfill_cnt != 759845 or candles_cnt != 284:
            print("FALHA: Métricas da base operacional divergem do estado esperado pré-migração!")
            sys.exit(1)


    # 3. Pré-condições do SQLite spool operacional
    print("\n[3/6] Verificando integridade do SQLite operacional (data/buffer/spool.db)...")
    spool_path = "c:/Users/fie23/Projetos/win-market-monitor/data/buffer/spool.db"
    conn_spool = sqlite3.connect(spool_path)
    cur_spool = conn_spool.cursor()
    cur_spool.execute("SELECT COUNT(*) FROM spool_ticks;")
    spool_ticks = cur_spool.fetchone()[0]
    cur_spool.execute("SELECT COUNT(*) FROM spool_ticks WHERE status = 'PENDING';")
    spool_pending = cur_spool.fetchone()[0]
    cur_spool.execute("SELECT last_captured_msc, last_replicated_msc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
    spool_ckp = cur_spool.fetchone()
    print(f"  spool_ticks total: {spool_ticks}")
    print(f"  spool_ticks pending: {spool_pending} (esperado: 0)")
    print(f"  spool checkpoints: {spool_ckp}")
    if spool_pending != 0 or spool_ticks != 2093221:
        print("FALHA: SQLite operacional possui pendências ou contagem divergente!")
        sys.exit(1)

    # 4. Criar novo backup operacional consistente e independente
    print("\n[4/6] Criando novo backup operacional independente pré-implantação...")
    backup_dir = "c:/Users/fie23/Projetos/win-market-monitor/data/backup/pre_migration_20261020_1435"
    os.makedirs(backup_dir, exist_ok=True)
    dump_file = os.path.join(backup_dir, "market_data_operational_pre_migration.dump")

    # Usar pg_dump via container docker win_market_postgres
    cmd = [
        "docker", "exec", "win_market_postgres",
        "pg_dump", "-U", "postgres", "-d", "market_data", "-F", "c", "-f", "/tmp/operational_pre_migration.dump"
    ]

    subprocess.run(cmd, check=True)

    # Copiar do container para o host
    cmd_copy = ["docker", "cp", "win_market_postgres:/tmp/operational_pre_migration.dump", dump_file]
    subprocess.run(cmd_copy, check=True)

    # Remover temporário do container
    subprocess.run(["docker", "exec", "win_market_postgres", "rm", "/tmp/operational_pre_migration.dump"], check=True)

    dump_size = os.path.getsize(dump_file)
    dump_hash = compute_sha256(dump_file)
    print(f"  Backup criado: {dump_file}")
    print(f"  Tamanho: {dump_size} bytes ({dump_size / (1024*1024):.2f} MB)")
    print(f"  SHA-256: {dump_hash}")

    # Também fazer backup do spool.db
    spool_backup_file = os.path.join(backup_dir, "spool_operational_pre_migration.db")
    with open(spool_path, "rb") as src, open(spool_backup_file, "wb") as dst:
        dst.write(src.read())
    spool_backup_hash = compute_sha256(spool_backup_file)
    print(f"  Spool backup criado: {spool_backup_file}")
    print(f"  Spool SHA-256: {spool_backup_hash}")

    # 5. Testar restauração do backup em base temporária no sandbox (5435)
    print("\n[5/6] Testando restauração do backup em banco temporário no Sandbox (5435)...")
    # Usar docker exec para drop e create da base de teste
    subprocess.run([
        "docker", "exec", "win_market_sandbox_postgres",
        "dropdb", "-U", "sandbox_user", "--if-exists", "market_data_restore_test"
    ], check=True)
    subprocess.run([
        "docker", "exec", "win_market_sandbox_postgres",
        "createdb", "-U", "sandbox_user", "market_data_restore_test"
    ], check=True)

    # Copiar dump para o container do sandbox
    subprocess.run(["docker", "cp", dump_file, "win_market_sandbox_postgres:/tmp/restore_test.dump"], check=True)
    # Restaurar
    res_restore = subprocess.run([
        "docker", "exec", "win_market_sandbox_postgres",
        "pg_restore", "-U", "sandbox_user", "-d", "market_data_restore_test", "--clean", "--if-exists", "/tmp/restore_test.dump"
    ], capture_output=True, text=True)
    subprocess.run(["docker", "exec", "win_market_sandbox_postgres", "rm", "/tmp/restore_test.dump"], check=True)

    # 6. Validar restauração de teste
    print("\n[6/6] Validando integridade absoluta da base restaurada de teste...")
    conn_restored = psycopg2.connect(
        host="127.0.0.1", port=5435, dbname="market_data_restore_test", user="sandbox_user", password="sandbox_pass"
    )
    with conn_restored.cursor() as cur:
        cur.execute("SELECT COUNT(*), SUM(volume_real) FROM market_ticks_raw_2026_10_19 WHERE symbol = 'WINZ26';")
        r_ticks, r_vol = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM candles_m2 WHERE symbol = 'WINZ26';")

        r_candles = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM market_ticks_raw_2026_10_19 WHERE provenance = 'RECOVERED_HISTORICAL_MT5';")
        r_backfill = cur.fetchone()[0]

    print(f"  Restaurado - Ticks 19/10: {r_ticks} (esperado: 2093221)")
    print(f"  Restaurado - Volume 19/10: {r_vol} (esperado: 19838713)")
    print(f"  Restaurado - Candles: {r_candles} (esperado: 284)")
    print(f"  Restaurado - Backfill: {r_backfill} (esperado: 759845)")

    if r_ticks != 2093221 or r_vol != 19838713 or r_candles != 284 or r_backfill != 759845:
        print("FALHA CRÍTICA: Restauração de teste divergiu da base operacional!")
        sys.exit(1)


    print("\n" + "=" * 80)
    print("ETAPA A CONCLUÍDA COM 100% DE SUCESSO!")
    print("Todas as pré-condições verificadas, backup íntegro e restauração comprovada.")
    print("=" * 80)

if __name__ == "__main__":
    main()
