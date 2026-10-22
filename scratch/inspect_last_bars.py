import sys
sys.path.insert(0, ".")
import psycopg2
import MetaTrader5 as mt5
from datetime import datetime, timezone
import pandas as pd
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

normalizer = MT5TimeNormalizer()

mt5.initialize()
t_start_market = datetime(2026, 10, 19, 9, 0, 0, tzinfo=normalizer.market_tz)
t_end_market = datetime(2026, 10, 19, 18, 35, 0, tzinfo=normalizer.market_tz)

rates = mt5.copy_rates_range(
    "WINZ26",
    mt5.TIMEFRAME_M2,
    normalizer.market_datetime_to_server_datetime(t_start_market),
    normalizer.market_datetime_to_server_datetime(t_end_market)
)
mt5.shutdown()

df_mt5 = pd.DataFrame(rates)
df_mt5['dt_utc'] = [normalizer.server_seconds_to_utc_datetime(s) for s in df_mt5['time']]
df_mt5['dt_brt'] = [dt.astimezone(normalizer.market_tz) for dt in df_mt5['dt_utc']]

print("Últimas 5 barras MT5:")
print(df_mt5[['dt_brt', 'open', 'high', 'low', 'close', 'tick_volume', 'real_volume']].tail(5))

conn_pg = psycopg2.connect("postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox")
cur_pg = conn_pg.cursor()
cur_pg.execute("""
    SELECT open_time_utc, close_time_utc, open, high, low, close, tick_volume, volume_real,
           state, revision_number, has_subsecond_ambiguity
    FROM candles_m2_v2
    ORDER BY open_time_utc DESC
    LIMIT 5;
""")
rows = cur_pg.fetchall()
conn_pg.close()

print("\nÚltimas 5 barras PG candles_m2_v2:")
for r in reversed(rows):
    dt_brt = r[0].astimezone(normalizer.market_tz)
    print(f"[{dt_brt}] O={r[2]} H={r[3]} L={r[4]} C={r[5]} Vol={r[7]} Trades={r[6]} Amb={r[10]}")
