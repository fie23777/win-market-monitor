import MetaTrader5 as mt5
from datetime import datetime, timezone
import psycopg2

if not mt5.initialize():
    print("MT5 init failed")
    exit(1)

rates = mt5.copy_rates_from_pos("WINZ26", mt5.TIMEFRAME_M2, 0, 300)
print(f"Total rates fetched from MT5: {len(rates)}")
target_minutes = [(13, 8), (13, 18), (13, 50), (14, 0), (14, 6)]

mt5_dict = {}
for r in rates:
    dt = datetime.fromtimestamp(r['time'], timezone.utc)
    if (dt.hour, dt.minute) in target_minutes:
        key = f"{dt.hour:02d}:{dt.minute:02d}"
        mt5_dict[key] = r

mt5.shutdown()

conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur = conn.cursor()

print("\n=== COMPARAÇÃO EXAUSTIVA CORRETA ===")
for key in sorted(mt5_dict.keys()):
    h, m = map(int, key.split(":"))
    t_start = f"2026-10-19 {h:02d}:{m:02d}:00-03"
    t_end = f"2026-10-19 {h:02d}:{m+2:02d}:00-03"
    
    # 1. Candle persistido
    cur.execute(f"""
        SELECT open, high, low, close, volume_real, tick_volume
        FROM candles_m2
        WHERE open_time_utc AT TIME ZONE 'America/Sao_Paulo' = '{t_start}'::timestamptz AT TIME ZONE 'America/Sao_Paulo';
    """)
    candle = cur.fetchone()
    
    # 2. Agg dos ticks
    cur.execute(f"""
        SELECT count(*), sum(volume_real)
        FROM market_ticks_raw
        WHERE time_utc >= '{t_start}'::timestamptz AND time_utc < '{t_end}'::timestamptz;
    """)
    ticks_agg = cur.fetchone()
    
    r_mt5 = mt5_dict[key]
    print(f"\nHorário: {key}")
    print(f"  Candle Persistido: OHLC=({candle[0]},{candle[1]},{candle[2]},{candle[3]}), Vol={candle[4]:.0f}, Ticks={candle[5]}")
    print(f"  PG Ticks Agg:      Vol={ticks_agg[1]:.0f}, Ticks={ticks_agg[0]}")
    print(f"  MT5 Atualizado:    OHLC=({r_mt5['open']},{r_mt5['high']},{r_mt5['low']},{r_mt5['close']}), Vol={r_mt5['real_volume']}, Ticks={r_mt5['tick_volume']}")
    print(f"  Déficit Candle vs PG Ticks: Vol={ticks_agg[1] - candle[4]:.0f}, Ticks={ticks_agg[0] - candle[5]}")
    print(f"  Déficit Candle vs MT5:      Vol={r_mt5['real_volume'] - candle[4]:.0f}, Ticks={r_mt5['tick_volume'] - candle[5]}")
    print(f"  Diferença MT5 vs PG Ticks:  Vol={r_mt5['real_volume'] - ticks_agg[1]:.0f}, Ticks={r_mt5['tick_volume'] - ticks_agg[0]}")

cur.close()
conn.close()
