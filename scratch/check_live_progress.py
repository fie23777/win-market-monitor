import psycopg2
import sqlite3
import zoneinfo
from datetime import datetime

BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

conn_pg = psycopg2.connect(host='127.0.0.1', port=5434, dbname='market_data', user='postgres', password='password')
cur_pg = conn_pg.cursor()

cur_pg.execute("SELECT count(*), max(time_msc) FROM market_ticks_raw WHERE time_utc >= '2026-10-19 00:00:00+00';")
pg_cnt, max_msc = cur_pg.fetchone()

cur_pg.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'RECOVERED_HISTORICAL_MT5' AND time_utc >= '2026-10-19 00:00:00+00';")
pg_hist = cur_pg.fetchone()[0]

cur_pg.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'REALTIME_STREAM' AND time_utc >= '2026-10-19 00:00:00+00';")
pg_rt = cur_pg.fetchone()[0]

cur_pg.execute("SELECT count(*) FROM candles_m2 WHERE open_time_utc >= '2026-10-19 00:00:00+00';")
pg_candles = cur_pg.fetchone()[0]

cur_pg.execute("SELECT time_msc, time_utc, price FROM market_ticks_raw WHERE time_utc >= '2026-10-19 00:00:00+00' ORDER BY time_msc DESC LIMIT 1;")
last_row = cur_pg.fetchone()

conn_pg.close()

conn_sq = sqlite3.connect('data/buffer/spool.db')
cur_sq = conn_sq.cursor()
cur_sq.execute("SELECT count(*) FROM spool_ticks;")
sq_cnt = cur_sq.fetchone()[0]
cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE status = 'PENDING';")
sq_pending = cur_sq.fetchone()[0]
cur_sq.execute("SELECT last_captured_msc, last_replicated_msc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
ckp = cur_sq.fetchone()
conn_sq.close()

print(f"=== PROGRESSO ATUAL DA SESSÃO SUPERVISIONADA ===")
print(f"PostgreSQL Total Ticks: {pg_cnt} (Histórico: {pg_hist}, Tempo Real: {pg_rt})")
print(f"SQLite Total Ticks:     {sq_cnt} (Pendentes de replicação: {sq_pending})")
print(f"Checkpoints Atuais:     Captured={ckp[0]} | Replicated={ckp[1]}")
print(f"Candles M2 Persistidos: {pg_candles}")
if last_row:
    dt_brt = last_row[1].astimezone(BRT_TZ)
    print(f"Último Tick Gravado:    MSC={last_row[0]} | BRT={dt_brt.strftime('%H:%M:%S.%f')[:-3]} | Preço={last_row[2]}")
