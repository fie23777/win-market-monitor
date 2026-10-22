import sys
sys.path.insert(0, ".")
import psycopg2
from datetime import datetime, timezone
import zoneinfo
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.reconciliation.tick_reconciler import TickReconciler

normalizer = MT5TimeNormalizer()
tz_brt = zoneinfo.ZoneInfo("America/Sao_Paulo")

dt_start_brt = datetime(2026, 10, 19, 9, 0, 0, tzinfo=tz_brt)
dt_end_brt = datetime(2026, 10, 19, 9, 5, 0, tzinfo=tz_brt)

start_utc = dt_start_brt.astimezone(timezone.utc)
end_utc = dt_end_brt.astimezone(timezone.utc)
start_msc = normalizer.utc_datetime_to_server_msc(start_utc)
end_msc = normalizer.utc_datetime_to_server_msc(end_utc)

print(f"Janela 1 teste: {dt_start_brt} a {dt_end_brt}")
print(f"Limites UTC: {start_utc} a {end_utc}")
print(f"Limites server_msc: {start_msc} a {end_msc}")

# 1. PostgreSQL ticks
conn_pg = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur_pg = conn_pg.cursor()
cur_pg.execute("""
    SELECT tick_fingerprint, time_msc, price, volume_real, flags, trade_side, occurrence_ordinal
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26' AND time_utc >= %s AND time_utc < %s
    ORDER BY time_msc ASC, occurrence_ordinal ASC;
""", (start_utc, end_utc))
pg_rows = cur_pg.fetchall()
cur_pg.close()
conn_pg.close()

print(f"Trades no PG na Janela 1: {len(pg_rows)}")

# 2. MT5 ticks
reader = MT5ReadOnlyReader(normalizer)
reader.initialize()
raw_mt5 = reader.fetch_ticks_range("WINZ26", dt_start_brt, dt_end_brt)
reader.shutdown()

reconciler = TickReconciler(time_normalizer=normalizer)
# Filtrar strictly time_msc na janela
raw_in_window = [t for t in raw_mt5 if start_msc <= t['time_msc'] < end_msc]
rec_mt5 = reconciler.reconcile_raw_ticks(raw_in_window, symbol="WINZ26", provenance="AUDIT")

print(f"Trades reconciliados MT5 na Janela 1: {len(rec_mt5)}")

pg_fps = set(r[0] for r in pg_rows)
mt5_fps = set(t.tick_fingerprint for t in rec_mt5)

print(f"Interseção: {len(pg_fps & mt5_fps)}")
print(f"Ausentes no PG (MT5 - PG): {len(mt5_fps - pg_fps)}")
print(f"Excedentes no PG (PG - MT5): {len(pg_fps - mt5_fps)}")
