import os
import sys
import sqlite3
import psycopg2
import psutil
from datetime import datetime, timezone, timedelta
import zoneinfo

def analyze():
    brt_tz = zoneinfo.ZoneInfo("America/Sao_Paulo")
    now_brt = datetime.now(brt_tz)
    print(f"=== CHECKPOINT DE ABERTURA - PILOTO 21/10/2026 ===")
    print(f"Horário da análise: {now_brt.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # 1 & 9. PID 4124
    print("\n--- 1 & 9. STATUS DO PROCESSO (PID 4124) ---")
    pid = 4124
    if psutil.pid_exists(pid):
        p = psutil.Process(pid)
        print(f"PID: {p.pid}")
        print(f"Nome: {p.name()}")
        print(f"Status: {p.status()}")
        print(f"Memória RSS: {p.memory_info().rss / (1024*1024):.2f} MB")
        print(f"CPU Percent: {p.cpu_percent(interval=0.5)}%")
        print(f"Criado em: {datetime.fromtimestamp(p.create_time(), brt_tz).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Linha de comando: {' '.join(p.cmdline())}")
    else:
        print(f"ALERTA: PID {pid} NÃO EXISTE!")

    # 3, 4, 5. SQLite dados de hoje (21/10)
    print("\n--- DADOS NO BUFFER LOCAL (SQLite spool.db) ---")
    sq_path = "data/buffer/spool.db"
    sq_conn = sqlite3.connect(sq_path)
    sq_cur = sq_conn.cursor()

    # Session start hoje: 1792573200000 msc
    session_start_msc = 1792573200000
    sq_cur.execute("SELECT count(*) FROM spool_ticks WHERE time_msc >= ?;", (session_start_msc,))
    today_ticks_spool = sq_cur.fetchone()[0]

    sq_cur.execute("SELECT count(*) FROM spool_ticks WHERE time_msc >= ? AND status = 'PENDING';", (session_start_msc,))
    today_pending_spool = sq_cur.fetchone()[0]

    sq_cur.execute("SELECT count(*) FROM spool_ticks WHERE time_msc >= ? AND status = 'COMMITTED';", (session_start_msc,))
    today_committed_spool = sq_cur.fetchone()[0]

    sq_cur.execute("SELECT min(time_msc), max(time_msc), min(created_at_utc), max(created_at_utc) FROM spool_ticks WHERE time_msc >= ?;", (session_start_msc,))
    msc_range = sq_cur.fetchone()

    # Checkpoints
    sq_cur.execute("SELECT stream_key, last_captured_msc, last_replicated_msc, watermark_complete_msc, updated_at_utc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
    ckp = sq_cur.fetchone()

    # Candles M2 no SQLite de hoje
    sq_cur.execute("SELECT count(*), min(open_time_msc), max(close_time_msc) FROM spool_candles_m2 WHERE open_time_msc >= ?;", (session_start_msc,))
    candles_spool = sq_cur.fetchone()

    print(f"Total Ticks sessão 21/10 no SQLite: {today_ticks_spool}")
    print(f"  Status PENDING: {today_pending_spool}")
    print(f"  Status COMMITTED: {today_committed_spool}")
    if msc_range[0]:
        t_min_brt = datetime.fromtimestamp(msc_range[0]/1000, timezone.utc).astimezone(brt_tz)
        t_max_brt = datetime.fromtimestamp(msc_range[1]/1000, timezone.utc).astimezone(brt_tz)
        print(f"  Primeiro tick: {msc_range[0]} msc ({t_min_brt.strftime('%H:%M:%S.%f')[:-3]} BRT)")
        print(f"  Último tick: {msc_range[1]} msc ({t_max_brt.strftime('%H:%M:%S.%f')[:-3]} BRT)")
    print(f"Candles M2 sessão 21/10 no SQLite: {candles_spool[0]}")
    if ckp:
        print(f"Checkpoints tabela:")
        print(f"  last_captured_msc: {ckp[1]}")
        print(f"  last_replicated_msc: {ckp[2]}")
        print(f"  watermark_complete_msc: {ckp[3]}")
        print(f"  updated_at_utc: {ckp[4]}")

    # Amostra dos primeiros 5 negócios de hoje
    print("\n--- 2. PRIMEIROS NEGÓCIOS DE HOJE NO SQLITE ---")
    sq_cur.execute("SELECT time_msc, price, volume_real, trade_side, flags, occurrence_ordinal, provenance FROM spool_ticks WHERE time_msc >= ? ORDER BY time_msc ASC, occurrence_ordinal ASC LIMIT 5;", (session_start_msc,))
    for row in sq_cur.fetchall():
        dt_brt = datetime.fromtimestamp(row[0]/1000, timezone.utc).astimezone(brt_tz)
        print(f"  {dt_brt.strftime('%H:%M:%S.%f')[:-3]} | Preço: {row[1]} | Vol: {row[2]} | Lado: {row[3]} | Flags: {row[4]} | Ord: {row[5]} | Prov: {row[6]}")

    # Verificar duplicatas de fingerprints no SQLite hoje
    sq_cur.execute("SELECT tick_fingerprint, count(*) FROM spool_ticks WHERE time_msc >= ? GROUP BY tick_fingerprint HAVING count(*) > 1;", (session_start_msc,))
    dups_spool = sq_cur.fetchall()
    print(f"Duplicatas de fingerprint no SQLite hoje: {len(dups_spool)}")

    sq_conn.close()

    # PostgreSQL
    print("\n--- DADOS NO POSTGRESQL 17 (market_data) ---")
    pg_conn = psycopg2.connect(
        host="127.0.0.1", port=5434, database="market_data", user="postgres", password="password"
    )
    pg_cur = pg_conn.cursor()

    # Partição de hoje
    pg_cur.execute("SELECT to_regclass('market_ticks_raw_2026_10_21');")
    part_exists = pg_cur.fetchone()[0]
    print(f"Partição market_ticks_raw_2026_10_21 criada: {part_exists is not None}")

    # Contagem de ticks hoje no PG
    pg_cur.execute("SELECT count(*) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc >= %s;", (session_start_msc,))
    today_ticks_pg = pg_cur.fetchone()[0]

    pg_cur.execute("SELECT min(time_msc), max(time_msc) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc >= %s;", (session_start_msc,))
    pg_range = pg_cur.fetchone()

    # Candles M2 hoje no PG
    start_today_iso = "2026-10-21 00:00:00+00"
    pg_cur.execute("SELECT count(*), min(open_time_utc), max(close_time_utc) FROM candles_m2 WHERE symbol = 'WINZ26' AND open_time_utc >= %s;", (start_today_iso,))
    candles_pg = pg_cur.fetchone()

    # Checar duplicatas de fingerprint no PG hoje
    pg_cur.execute("SELECT tick_fingerprint, count(*) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc >= %s GROUP BY tick_fingerprint HAVING count(*) > 1;", (session_start_msc,))
    dups_pg = pg_cur.fetchall()

    print(f"Total Ticks sessão 21/10 no PostgreSQL: {today_ticks_pg}")
    if pg_range[0]:
        t_pg_min = datetime.fromtimestamp(pg_range[0]/1000, timezone.utc).astimezone(brt_tz)
        t_pg_max = datetime.fromtimestamp(pg_range[1]/1000, timezone.utc).astimezone(brt_tz)
        print(f"  Primeiro tick PG: {pg_range[0]} msc ({t_pg_min.strftime('%H:%M:%S.%f')[:-3]} BRT)")
        print(f"  Último tick PG: {pg_range[1]} msc ({t_pg_max.strftime('%H:%M:%S.%f')[:-3]} BRT)")
    print(f"Total Candles M2 hoje no PG: {candles_pg[0]} (min: {candles_pg[1]}, max: {candles_pg[2]})")
    print(f"Duplicatas no PostgreSQL hoje: {len(dups_pg)}")

    # Últimos candles M2 consolidados
    print("\n--- 7. ÚLTIMOS CANDLES M2 CONSOLIDADOS ---")
    pg_cur.execute("""
        SELECT open_time_utc, close_time_utc, open, high, low, close, volume_real, tick_volume, is_closed
        FROM candles_m2
        WHERE symbol = 'WINZ26' AND open_time_utc >= %s
        ORDER BY open_time_utc DESC LIMIT 5;
    """, (start_today_iso,))
    for c_row in pg_cur.fetchall():
        open_brt = c_row[0].astimezone(brt_tz).strftime('%H:%M')
        close_brt = c_row[1].astimezone(brt_tz).strftime('%H:%M')
        print(f"  Candle [{open_brt} - {close_brt} BRT]: O={c_row[2]} H={c_row[3]} L={c_row[4]} C={c_row[5]} Vol={c_row[6]} Ticks={c_row[7]} Closed={c_row[8]}")

    pg_conn.close()

if __name__ == "__main__":
    analyze()
