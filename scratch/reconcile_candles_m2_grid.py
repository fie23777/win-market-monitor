import sys
sys.path.insert(0, ".")
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

normalizer = MT5TimeNormalizer()

snapshot_candles_csv = Path("F:/win-market-monitor-backup/session_closing_snapshot_20261019_214726/candles_m2_snapshot.csv")

print("=== RECONCILIAÇÃO TEMPORAL DOS 284 CANDLES M2 COM A GRADE TEMPORAL ===")

df = pd.read_csv(snapshot_candles_csv)
print(f"Total candles carregados: {len(df)}")

df['open_time_utc'] = pd.to_datetime(df['open_time_utc'])
df['close_time_utc'] = pd.to_datetime(df['close_time_utc'])
df = df.sort_values('open_time_utc').reset_index(drop=True)

# Converter para BRT
df['open_time_brt'] = df['open_time_utc'].dt.tz_convert(normalizer.market_tz)
df['close_time_brt'] = df['close_time_utc'].dt.tz_convert(normalizer.market_tz)

print(f"Primeiro candle: open={df.iloc[0]['open_time_brt']} close={df.iloc[0]['close_time_brt']}")
print(f"Último candle:   open={df.iloc[-1]['open_time_brt']} close={df.iloc[-1]['close_time_brt']}")

# Grade esperada:
# De 09:00:00 até 18:30:00 em passos de 2 minutos
# 09:00 -> 09:02 ... até 18:28 -> 18:30 (285 candles se for até 18:30, ou 286 se 18:30->18:32)
# Vamos gerar a grade completa de 09:00 até 18:32 (intervalos de 2 min)
start_dt = datetime(2026, 10, 19, 9, 0, 0, tzinfo=normalizer.market_tz)
end_dt = datetime(2026, 10, 19, 18, 32, 0, tzinfo=normalizer.market_tz)

expected_slots = []
curr = start_dt
while curr < end_dt:
    expected_slots.append(curr)
    curr += timedelta(minutes=2)

print(f"\nSlots M2 esperados entre 09:00 e 18:32 BRT: {len(expected_slots)} slots")

# Mapear slots presentes
present_slots = set(df['open_time_brt'])

missing_slots = [s for s in expected_slots if s not in present_slots]
print(f"Slots ausentes na grade ({len(missing_slots)} slots):")
for s in missing_slots:
    print(f"  Slot ausente: {s.strftime('%Y-%m-%d %H:%M:%S %Z')}")

# Verificar deltas entre candles consecutivos
deltas = df['open_time_brt'].diff()
anomalous_deltas = df[deltas > timedelta(minutes=2)]
if not anomalous_deltas.empty:
    print("\nCandles com gap anterior maior que 2 minutos:")
    for idx, row in anomalous_deltas.iterrows():
        prev_row = df.iloc[idx-1]
        print(f"  Gap entre {prev_row['open_time_brt'].strftime('%H:%M:%S')} e {row['open_time_brt'].strftime('%H:%M:%S')} (Delta = {deltas.iloc[idx]})")
else:
    print("\nNenhum salto inesperado no meio do fluxo.")

# Analisar os últimos 5 candles
print("\nÚltimos 5 candles da sessão:")
for idx in range(max(0, len(df)-5), len(df)):
    r = df.iloc[idx]
    print(f"  Candle #{idx+1}: Open={r['open_time_brt'].strftime('%H:%M:%S')} Close={r['close_time_brt'].strftime('%H:%M:%S')} VolReal={r['volume_real']} Ticks={r['tick_volume']} is_closed={r['is_closed']} prov={r['provenance']}")
