import MetaTrader5 as mt5
from datetime import datetime, timezone
import psycopg2
import pandas as pd

if not mt5.initialize():
    print("MT5 init failed")
    exit(1)

# Fetch ticks from MT5 for [14:00:00.000, 14:02:00.000)
# MT5 copy_ticks_range with UTC tzinfo
t_start_dt = datetime(2026, 10, 19, 14, 0, 0, tzinfo=timezone.utc)
t_end_dt = datetime(2026, 10, 19, 14, 2, 0, tzinfo=timezone.utc)

ticks_mt5 = mt5.copy_ticks_range("WINZ26", t_start_dt, t_end_dt, mt5.COPY_TICKS_TRADE)
print(f"Total MT5 trade ticks in range [14:00:00, 14:02:00): {len(ticks_mt5)}")

mt5.shutdown()

# Connect to Postgres
conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur = conn.cursor()

# Query PG ticks for the same interval
cur.execute("""
    SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, tick_fingerprint,
           time_utc AT TIME ZONE 'America/Sao_Paulo' as brt
    FROM market_ticks_raw
    WHERE time_utc >= '2026-10-19 14:00:00-03'::timestamptz
      AND time_utc < '2026-10-19 14:02:00-03'::timestamptz
    ORDER BY time_msc, occurrence_ordinal;
""")
pg_ticks = cur.fetchall()
print(f"Total PG ticks in range [14:00:00, 14:02:00): {len(pg_ticks)}")

# Let's inspect MT5 total volume vs PG total volume
vol_mt5 = sum(t['volume_real'] for t in ticks_mt5)
vol_pg = sum(t[2] for t in pg_ticks)
print(f"MT5 Trade Ticks Count: {len(ticks_mt5)}, Vol: {vol_mt5}")
print(f"PG Trade Ticks Count:  {len(pg_ticks)}, Vol: {vol_pg}")

# Check difference between MT5 trade ticks and PG trade ticks
# Find which tick is in MT5 but not PG, or vice-versa
df_mt5 = pd.DataFrame(ticks_mt5)
df_pg = pd.DataFrame(pg_ticks, columns=['time_msc', 'price', 'volume_real', 'flags', 'trade_side', 'occurrence_ordinal', 'tick_fingerprint', 'brt'])

print("\n--- MT5 FIRST 3 TICKS ---")
print(df_mt5[['time_msc', 'last', 'volume_real', 'flags']].head(3))

print("\n--- PG FIRST 3 TICKS ---")
print(df_pg[['time_msc', 'price', 'volume_real', 'flags', 'occurrence_ordinal']].head(3))

print("\n--- MT5 LAST 3 TICKS ---")
print(df_mt5[['time_msc', 'last', 'volume_real', 'flags']].tail(3))

print("\n--- PG LAST 3 TICKS ---")
print(df_pg[['time_msc', 'price', 'volume_real', 'flags', 'occurrence_ordinal']].tail(3))

cur.close()
conn.close()
