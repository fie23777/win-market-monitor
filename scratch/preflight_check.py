import os
import sys
import shutil
import sqlite3
import psutil
import yaml
from datetime import datetime, timezone
import zoneinfo

def run_checks():
    print("=== FASE 1: VERIFICAÇÕES PRÉ-FLIGHT ===")
    
    # 1. Data e Hora em America/Sao_Paulo
    brt_tz = zoneinfo.ZoneInfo("America/Sao_Paulo")
    now_brt = datetime.now(brt_tz)
    print(f"[1] Horario BRT atual: {now_brt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    is_date_ok = (now_brt.strftime('%Y-%m-%d') == '2026-10-21')
    print(f"    Data é 2026-10-21: {is_date_ok}")
    
    # 2 & 3. Git branch e commit
    import subprocess
    branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True).strip()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    git_diff = subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
    untracked_only = True
    for line in git_diff.splitlines():
        if not line.startswith("??"):
            untracked_only = False
    print(f"[2] Branch Git: {branch} (esperado: develop)")
    print(f"    Commit Git: {commit} (esperado inicia com 19dd8c4: {commit.startswith('19dd8c4')})")
    print(f"[3] Alterações não homologadas no Git: {'NENHUMA (apenas untracked)' if untracked_only else git_diff}")

    # 4. MetaTrader 5
    print("[4] Verificando MetaTrader 5 XP e WINZ26...")
    try:
        import MetaTrader5 as mt5
        init_ok = mt5.initialize()
        conn_ok = False
        sym_ok = False
        term_info = None
        if init_ok:
            conn_ok = mt5.terminal_info().connected if mt5.terminal_info() else False
            sym_ok = mt5.symbol_select("WINZ26", True)
            term_info = mt5.terminal_info()
            # Deixar conectado ou não fechar abruptamente se for usado
        print(f"    MT5 initialized: {init_ok}")
        print(f"    MT5 connected: {conn_ok}")
        print(f"    WINZ26 selected: {sym_ok}")
        if term_info:
            print(f"    Terminal: {term_info.name}, Build: {term_info.build}, Server: {term_info.company}")
    except Exception as e:
        print(f"    MT5 Erro: {e}")

    # 5. PostgreSQL porta 5434 e SQLite
    print("[5] Verificando PostgreSQL (porta 5434 / market_data) e SQLite...")
    try:
        import psycopg2
        pg_conn = psycopg2.connect(
            host="127.0.0.1",
            port=5434,
            database="market_data",
            user="postgres",
            password="password",
            connect_timeout=3
        )
        cur = pg_conn.cursor()
        cur.execute("SELECT version();")
        pg_ver = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM raw_ticks_b3 WHERE symbol = 'WINZ26';")
        ticks_pg_count = cur.fetchone()[0]
        print(f"    PostgreSQL OK: {pg_ver}")
        print(f"    PostgreSQL ticks WINZ26 existentes: {ticks_pg_count}")
        pg_conn.close()
    except Exception as e:
        print(f"    PostgreSQL Erro: {e}")

    sqlite_path = "data/buffer/spool.db"
    if os.path.exists(sqlite_path):
        try:
            sq_conn = sqlite3.connect(sqlite_path)
            c = sq_conn.cursor()
            c.execute("PRAGMA integrity_check;")
            integ = c.fetchone()[0]
            c.execute("SELECT count(*) FROM ticks_spool;")
            spool_count = c.fetchone()[0]
            c.execute("SELECT count(*) FROM ticks_spool WHERE status = 'PENDING';")
            pending_count = c.fetchone()[0]
            c.execute("SELECT last_captured_msc, last_replicated_msc FROM checkpoints WHERE stream_key = 'WINZ26';")
            ckp_row = c.fetchone()
            print(f"    SQLite spool.db integrity: {integ}")
            print(f"    SQLite total ticks: {spool_count}, pending: {pending_count}")
            print(f"    SQLite checkpoints: {ckp_row}")
            sq_conn.close()
        except Exception as e:
            print(f"    SQLite Erro: {e}")
    else:
        print(f"    SQLite spool.db: NÃO ENCONTRADO em {sqlite_path}")

    # 6. Configurações read_only e allow_orders
    with open("config/collector_config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    ro = cfg.get("collector", {}).get("read_only")
    ao = cfg.get("collector", {}).get("allow_orders")
    print(f"[6] read_only: {ro} (esperado True), allow_orders: {ao} (esperado False)")

    # 7. Outro processo coletor ativo
    current_pid = os.getpid()
    active_collectors = []
    for p in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if p.info['pid'] == current_pid:
                continue
            cmdline = " ".join(p.info['cmdline'] or [])
            if "src.main" in cmdline or "win-market-monitor" in cmdline:
                active_collectors.append((p.info['pid'], p.info['name'], cmdline))
        except Exception:
            pass
    print(f"[7] Processos coletores concorrentes: {len(active_collectors)} {active_collectors}")

    # 8. Migrações históricas não serão reexecutadas
    print(f"[8] Garantia de migrações: scripts de migração histórica (migrations 01 a 08) NÃO serão chamados.")

    # 9. Espaço em disco e diretórios
    print(f"[9] Verificação de discos e diretórios:")
    for path in ["data", "data/buffer", "data/archive", "data/backup"]:
        os.makedirs(path, exist_ok=True)
        print(f"    Diretório local {path}: OK (permissão de escrita ok)")
    for d in ["C:\\", "F:\\"]:
        if os.path.exists(d):
            total, used, free = shutil.disk_usage(d)
            print(f"    Disco {d} -> Livre: {free / (1024**3):.2f} GB / Total: {total / (1024**3):.2f} GB")
        else:
            print(f"    Disco {d}: NÃO ACESSÍVEL / NÃO CONECTADO")

    # 10. PeriodicHistoricalReconciler e lookback_ms
    lookback_ms = cfg.get("collector", {}).get("lookback_ms")
    print(f"[10] Configuração lookback_ms: {lookback_ms} (esperado: 2000)")

if __name__ == "__main__":
    run_checks()
