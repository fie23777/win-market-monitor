import sys
sys.path.insert(0, ".")
import psycopg2
import MetaTrader5 as mt5
from datetime import datetime, timezone
import pandas as pd
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

normalizer = MT5TimeNormalizer()

# Conectar MT5
if not mt5.initialize():
    print("MT5 Init Failed:", mt5.last_error())
    sys.exit(1)

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

# Conectar Sandbox PG
conn_pg = psycopg2.connect("postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox")
cur_pg = conn_pg.cursor()

# Inspecionar colunas
cur_pg.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'candles_m2_v2';")
cols = [r[0] for r in cur_pg.fetchall()]
print("Colunas candles_m2_v2:", cols)

cur_pg.execute("""
    SELECT open_time_utc, close_time_utc, open, high, low, close, tick_volume, volume_real,
           state, revision_number, has_subsecond_ambiguity
    FROM candles_m2_v2
    ORDER BY open_time_utc ASC;
""")
pg_rows = cur_pg.fetchall()
conn_pg.close()

df_pg = pd.DataFrame(pg_rows, columns=[
    'open_time_utc', 'close_time_utc', 'open', 'high', 'low', 'close', 'tick_volume', 'real_volume',
    'state', 'revision_number', 'has_subsecond_ambiguity'
])
df_pg['dt_brt'] = [dt.astimezone(normalizer.market_tz) for dt in df_pg['open_time_utc']]

print(f"\nTotal MT5: {len(df_mt5)} | Total PG candles_m2_v2: {len(df_pg)}")
set_mt5 = set(df_mt5['dt_brt'])
set_pg = set(df_pg['dt_brt'])

only_pg = sorted(list(set_pg - set_mt5))
only_mt5 = sorted(list(set_mt5 - set_pg))
print(f"Candles apenas no PG: {len(only_pg)} -> {only_pg}")
print(f"Candles apenas no MT5: {len(only_mt5)} -> {only_mt5}")

# Se houver apenas no PG, verificar o que tem neles
if only_pg:
    print("\nDetalhes dos candles que existem apenas no PG:")
    for dt in only_pg:
        sub = df_pg[df_pg['dt_brt'] == dt]
        print(sub[['dt_brt', 'open', 'high', 'low', 'close', 'tick_volume', 'real_volume', 'has_subsecond_ambiguity']])

# Comparar candle a candle nos comuns
common_dts = sorted(list(set_mt5.intersection(set_pg)))
print(f"\nComparando {len(common_dts)} candles em comum:")

divergences = []
for dt in common_dts:
    r_mt5 = df_mt5[df_mt5['dt_brt'] == dt].iloc[0]
    r_pg = df_pg[df_pg['dt_brt'] == dt].iloc[0]
    
    diff_open = r_mt5['open'] != r_pg['open']
    diff_high = r_mt5['high'] != r_pg['high']
    diff_low = r_mt5['low'] != r_pg['low']
    diff_close = r_mt5['close'] != r_pg['close']
    diff_vol = r_mt5['real_volume'] != r_pg['real_volume']
    diff_ticks = r_mt5['tick_volume'] != r_pg['tick_volume']
    
    if diff_open or diff_high or diff_low or diff_close or diff_vol or diff_ticks:
        divergences.append({
            'dt_brt': dt,
            'mt5_ohlcv': (r_mt5['open'], r_mt5['high'], r_mt5['low'], r_mt5['close'], r_mt5['real_volume'], r_mt5['tick_volume']),
            'pg_ohlcv': (r_pg['open'], r_pg['high'], r_pg['low'], r_pg['close'], r_pg['real_volume'], r_pg['tick_volume']),
            'diffs': {
                'open': (r_mt5['open'], r_pg['open']) if diff_open else None,
                'high': (r_mt5['high'], r_pg['high']) if diff_high else None,
                'low': (r_mt5['low'], r_pg['low']) if diff_low else None,
                'close': (r_mt5['close'], r_pg['close']) if diff_close else None,
                'vol': (r_mt5['real_volume'], r_pg['real_volume']) if diff_vol else None,
                'tick_vol': (r_mt5['tick_volume'], r_pg['tick_volume']) if diff_ticks else None,
            },
            'has_ambiguity': r_pg['has_subsecond_ambiguity']
        })

print(f"Total de candles com alguma divergência: {len(divergences)} de {len(common_dts)}")
if divergences:
    print(f"Primeiras 5 divergências:")
    for d in divergences[:5]:
        print(f"  [{d['dt_brt']}] Diffs: {d['diffs']} | Ambiguity: {d['has_ambiguity']}")
