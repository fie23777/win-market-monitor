import MetaTrader5 as mt5
from datetime import datetime
import pytz
import pandas as pd
import psycopg2

if not mt5.initialize():
    print("MT5 Init Failed:", mt5.last_error())
    exit(1)

sp_tz = pytz.timezone('America/Sao_Paulo')
t_start = sp_tz.localize(datetime(2026, 10, 19, 8, 50, 0))
t_end = sp_tz.localize(datetime(2026, 10, 19, 18, 40, 0))

rates = mt5.copy_rates_range('WINZ26', mt5.TIMEFRAME_M2, t_start, t_end)
df_mt5 = pd.DataFrame(rates)
if not df_mt5.empty:
    df_mt5['dt_utc'] = pd.to_datetime(df_mt5['time'], unit='s', utc=True)
    df_mt5['dt_sp'] = df_mt5['dt_utc'].dt.tz_convert('America/Sao_Paulo')
    print("MT5 Rates range:", df_mt5['dt_sp'].min(), "a", df_mt5['dt_sp'].max())
    print("MT5 Total bars:", len(df_mt5))
    print("MT5 Total real_volume:", df_mt5['real_volume'].sum())
    print("MT5 Total tick_volume:", df_mt5['tick_volume'].sum())
    print("\nPrimeiras 3 barras MT5:")
    print(df_mt5[['dt_sp', 'open', 'high', 'low', 'close', 'tick_volume', 'real_volume']].head(3))
    print("\nÚltimas 3 barras MT5:")
    print(df_mt5[['dt_sp', 'open', 'high', 'low', 'close', 'tick_volume', 'real_volume']].tail(3))

mt5.shutdown()

# Conectar no Sandbox PostgreSQL porta 5435
conn = psycopg2.connect(
    dbname="market_data_sandbox",
    user="sandbox_user",
    password="sandbox_password",
    host="127.0.0.1",
    port=5435
)
cur = conn.cursor()
cur.execute("SELECT count(*), min(bucket_start_utc), max(bucket_start_utc), sum(real_volume), sum(tick_volume) FROM candles_m2;")
c_m2 = cur.fetchone()
print(f"\nPostgreSQL candles_m2 (original snapshot): count={c_m2[0]}, min={c_m2[1]}, max={c_m2[2]}, sum_vol={c_m2[3]}, sum_ticks={c_m2[4]}")

cur.execute("SELECT count(*), min(bucket_start_utc), max(bucket_start_utc), sum(real_volume), sum(tick_volume) FROM candles_m2_v2;")
c_m2_v2 = cur.fetchone()
print(f"PostgreSQL candles_m2_v2 (reconstruído Fase C): count={c_m2_v2[0]}, min={c_m2_v2[1]}, max={c_m2_v2[2]}, sum_vol={c_m2_v2[3]}, sum_ticks={c_m2_v2[4]}")

conn.close()
