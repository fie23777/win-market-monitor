import os
import sys
import shutil
import sqlite3
import psutil
import yaml
from datetime import datetime, timezone
import zoneinfo

def run_detailed_status():
    print("=== STATUS DETALHADO DO SISTEMA APÓS INICIALIZAÇÃO ===")
    
    brt_tz = zoneinfo.ZoneInfo("America/Sao_Paulo")
    now_brt = datetime.now(brt_tz)
    print(f"Data/Hora BRT: {now_brt.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # Processo do Coletor
    collector_procs = []
    for p in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_info']):
        try:
            cmdline = " ".join(p.info['cmdline'] or [])
            if "src.main" in cmdline and "python" in p.info['name'].lower():
                collector_procs.append(p.info)
        except Exception:
            pass
    print(f"Processo Coletor Ativo: {len(collector_procs)} processo(s)")
    for cp in collector_procs:
        mem_mb = cp['memory_info'].rss / (1024 * 1024) if cp.get('memory_info') else 0
        print(f"  PID: {cp['pid']} | Nome: {cp['name']} | Memória RSS: {mem_mb:.2f} MB")

    # PostgreSQL
    try:
        import psycopg2
        pg_conn = psycopg2.connect(
            host="127.0.0.1", port=5434, database="market_data", user="postgres", password="password"
        )
        cur = pg_conn.cursor()
        cur.execute("SELECT count(*) FROM market_ticks_raw WHERE symbol = 'WINZ26';")
        ticks_pg = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM candles_m2 WHERE symbol = 'WINZ26';")
        candles_pg = cur.fetchone()[0]
        print(f"PostgreSQL 17 (market_data):")
        print(f"  Total Ticks (market_ticks_raw): {ticks_pg}")
        print(f"  Total Candles M2 (candles_m2): {candles_pg}")
        pg_conn.close()
    except Exception as e:
        print(f"PostgreSQL Erro: {e}")

    # SQLite
    sqlite_path = "data/buffer/spool.db"
    if os.path.exists(sqlite_path):
        try:
            sq_conn = sqlite3.connect(sqlite_path)
            c = sq_conn.cursor()
            c.execute("PRAGMA integrity_check;")
            integ = c.fetchone()[0]
            c.execute("SELECT count(*) FROM spool_ticks;")
            spool_total = c.fetchone()[0]
            c.execute("SELECT count(*) FROM spool_ticks WHERE status = 'PENDING';")
            pending_count = c.fetchone()[0]
            c.execute("SELECT count(*) FROM spool_ticks WHERE status = 'COMMITTED';")
            committed_count = c.fetchone()[0]
            c.execute("SELECT count(*) FROM spool_candles_m2;")
            candles_spool = c.fetchone()[0]
            c.execute("SELECT stream_key, last_captured_msc, last_replicated_msc, watermark_complete_msc, updated_at_utc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
            ckp = c.fetchone()
            print(f"SQLite (spool.db):")
            print(f"  Integridade física: {integ}")
            print(f"  Total Ticks no spool: {spool_total}")
            print(f"  Backlog PENDING: {pending_count}")
            print(f"  COMMITTED: {committed_count}")
            print(f"  Candles M2 no spool: {candles_spool}")
            if ckp:
                print(f"  Checkpoints no banco:")
                print(f"    Stream Key: {ckp[0]}")
                print(f"    W_captured (last_captured_msc): {ckp[1]}")
                print(f"    W_durable (last_replicated_msc): {ckp[2]}")
                print(f"    W_complete (watermark_complete_msc): {ckp[3]}")
                print(f"    Última atualização: {ckp[4]}")
            sq_conn.close()
        except Exception as e:
            print(f"SQLite Erro: {e}")

    # MT5
    try:
        import MetaTrader5 as mt5
        if mt5.initialize():
            t_info = mt5.terminal_info()
            sym_info = mt5.symbol_info("WINZ26")
            print(f"MetaTrader 5:")
            print(f"  Conectado: {t_info.connected if t_info else False}")
            print(f"  Servidor: {t_info.company if t_info else 'N/A'}")
            print(f"  Símbolo WINZ26 disponível: {sym_info is not None}")
            if sym_info:
                print(f"  WINZ26 trade_mode: {sym_info.trade_mode}")
    except Exception as e:
        print(f"MT5 Erro: {e}")

if __name__ == "__main__":
    run_detailed_status()
