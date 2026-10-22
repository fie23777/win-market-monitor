import sys
sys.path.insert(0, ".")
import psycopg2
import MetaTrader5 as mt5
from datetime import datetime, timezone
import pandas as pd
import numpy as np
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

normalizer = MT5TimeNormalizer()

# Conectar MT5
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

# Conectar Sandbox PG
conn_pg = psycopg2.connect("postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox")
cur_pg = conn_pg.cursor()
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

print("================================================================================")
print("  AUDITORIA FORENSE OHLCV: CANDLES_M2_V2 vs RATES NATIVOS MT5")
print("================================================================================")

# 1. Análise de Barras e Volumes Globais
vol_mt5_total = df_mt5['real_volume'].sum()
vol_pg_total = df_pg['real_volume'].sum()
print(f"Total de barras MT5: {len(df_mt5)} | Volume Total MT5: {vol_mt5_total:,.0f}")
print(f"Total de barras PG:  {len(df_pg)}  | Volume Total PG:  {vol_pg_total:,.0f}")
print(f"Delta de Volume Global: {vol_pg_total - vol_mt5_total:,.0f} contratos")

# 2. Mapeamento das barras
# Notamos que o candle 18:30 do PG (leilão) foi incorporado na barra 18:24 pelo MT5
# Vamos criar uma visão ajustada para comparar a barra 18:24 agregada
# E comparar os outros 282 candles individualmente
results = []
discrepancies = []

# Mapear MT5 por dt_brt
mt5_by_dt = {row['dt_brt']: row for _, row in df_mt5.iterrows()}
pg_by_dt = {row['dt_brt']: row for _, row in df_pg.iterrows()}

# Verificar candles de 09:00 até 18:22 (282 candles)
exact_ohlcv_matches = 0
boundary_tick_shifts = 0

for _, r_pg in df_pg.iterrows():
    dt = r_pg['dt_brt']
    if dt == datetime(2026, 10, 19, 18, 30, tzinfo=normalizer.market_tz):
        # Candle de leilão
        continue
    
    if dt not in mt5_by_dt:
        print(f"Candle {dt} ausente no MT5!")
        continue
        
    r_mt5 = mt5_by_dt[dt]
    
    # Se for 18:24, MT5 agregou o leilão
    if dt == datetime(2026, 10, 19, 18, 24, tzinfo=normalizer.market_tz):
        pg_1830 = pg_by_dt[datetime(2026, 10, 19, 18, 30, tzinfo=normalizer.market_tz)]
        combined_vol = r_pg['real_volume'] + pg_1830['real_volume']
        vol_match = (combined_vol == r_mt5['real_volume'])
        o_match = (r_pg['open'] == r_mt5['open'])
        # close no MT5 reflete o leilão
        c_match = (pg_1830['close'] == r_mt5['close'])
        h_match = (max(r_pg['high'], pg_1830['high']) == r_mt5['high'])
        l_match = (min(r_pg['low'], pg_1830['low']) == r_mt5['low'])
        results.append({
            'dt': dt,
            'status': 'AUCTION_MERGED_IN_MT5',
            'o_match': o_match,
            'h_match': h_match,
            'l_match': l_match,
            'c_match': c_match,
            'vol_match': vol_match,
            'details': f"PG 18:24 ({r_pg['real_volume']}) + PG 18:30 ({pg_1830['real_volume']}) = {combined_vol} vs MT5 18:24 ({r_mt5['real_volume']})"
        })
        continue

    # Barras regulares 09:00 até 18:22
    o_match = (r_pg['open'] == r_mt5['open'])
    h_match = (r_pg['high'] == r_mt5['high'])
    l_match = (r_pg['low'] == r_mt5['low'])
    c_match = (r_pg['close'] == r_mt5['close'])
    v_match = (r_pg['real_volume'] == r_mt5['real_volume'])
    
    if o_match and h_match and l_match and c_match and v_match:
        exact_ohlcv_matches += 1
        results.append({'dt': dt, 'status': 'EXACT_MATCH'})
    else:
        boundary_tick_shifts += 1
        diff_info = {
            'dt': dt,
            'status': 'BOUNDARY_VARIATION',
            'open': (r_mt5['open'], r_pg['open']) if not o_match else 'MATCH',
            'high': (r_mt5['high'], r_pg['high']) if not h_match else 'MATCH',
            'low': (r_mt5['low'], r_pg['low']) if not l_match else 'MATCH',
            'close': (r_mt5['close'], r_pg['close']) if not c_match else 'MATCH',
            'vol_mt5': r_mt5['real_volume'],
            'vol_pg': r_pg['real_volume'],
            'vol_delta': r_pg['real_volume'] - r_mt5['real_volume'],
            'ambiguity': r_pg['has_subsecond_ambiguity']
        }
        discrepancies.append(diff_info)
        results.append(diff_info)

print(f"\nResumo da Auditoria:")
print(f"  Candles Exatos OHLCV (100% idênticos): {exact_ohlcv_matches}")
print(f"  Candles com Variação de Fronteira ms:  {boundary_tick_shifts}")
print(f"  Candles de Leilão Agrupados no MT5:   1 (18:24 absorveu 18:30 no MT5)")
print(f"  Total Auditado:                       {exact_ohlcv_matches + boundary_tick_shifts + 1} de 284")

print(f"\nPrimeiras 10 Variações de Fronteira (Micro-alocação de ms):")
for d in discrepancies[:10]:
    print(f"  [{d['dt'].strftime('%H:%M')}] O:{d['open']} H:{d['high']} L:{d['low']} C:{d['close']} | Vol MT5={d['vol_mt5']}, PG={d['vol_pg']} (delta={d['vol_delta']}) | Amb={d['ambiguity']}")

# Verificar se a soma dos deltas de volume das variações é ZERO
total_vol_delta_variations = sum(d['vol_delta'] for d in discrepancies)
print(f"\nSoma dos deltas de volume das variações de fronteira: {total_vol_delta_variations:.0f} contratos")

# Quantos candles têm subsecond ambiguity?
cur_pg = conn_pg.cursor() if not conn_pg.closed else psycopg2.connect("postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox").cursor()
cur_pg.execute("SELECT count(*) FROM candles_m2_v2 WHERE has_subsecond_ambiguity = TRUE;")
amb_count = cur_pg.fetchone()[0]
print(f"Total de candles M2 com has_subsecond_ambiguity = TRUE: {amb_count} de 284")
