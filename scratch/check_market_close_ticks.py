import sys
sys.path.insert(0, ".")
import psycopg2
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

norm = MT5TimeNormalizer()
conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur = conn.cursor()

# Buscar ticks entre 18:24 e 18:35 BRT (21:24 e 21:35 UTC)
cur.execute("""
    SELECT time_msc, price, volume_real, flags, trade_side, time_utc 
    FROM market_ticks_raw 
    WHERE time_utc >= '2026-10-19 21:24:00+00' AND time_utc <= '2026-10-19 21:35:00+00' 
    ORDER BY time_msc ASC, occurrence_ordinal ASC;
""")
rows = cur.fetchall()
print(f"Total ticks entre 18:24 e 18:35 BRT: {len(rows)}")

ticks_before_auction = [r for r in rows if norm.server_msc_to_utc_datetime(r[0]).astimezone(norm.market_tz).minute < 30]
ticks_auction = [r for r in rows if norm.server_msc_to_utc_datetime(r[0]).astimezone(norm.market_tz).minute >= 30]

print(f"Ticks antes das 18:30: {len(ticks_before_auction)}")
if ticks_before_auction:
    last_before = ticks_before_auction[-1]
    dt_last_before = norm.server_msc_to_utc_datetime(last_before[0]).astimezone(norm.market_tz)
    print(f"  Último tick contínuo: {dt_last_before.strftime('%H:%M:%S.%f')[:-3]} Preço: {last_before[1]} Vol: {last_before[2]} Flags: {last_before[3]}")

print(f"Ticks do Leilão (>= 18:30): {len(ticks_auction)}")
if ticks_auction:
    first_auc = ticks_auction[0]
    last_auc = ticks_auction[-1]
    dt_first_auc = norm.server_msc_to_utc_datetime(first_auc[0]).astimezone(norm.market_tz)
    dt_last_auc = norm.server_msc_to_utc_datetime(last_auc[0]).astimezone(norm.market_tz)
    print(f"  Primeiro tick leilão: {dt_first_auc.strftime('%H:%M:%S.%f')[:-3]} Preço: {first_auc[1]} Vol: {first_auc[2]} Flags: {first_auc[3]}")
    print(f"  Último tick leilão:   {dt_last_auc.strftime('%H:%M:%S.%f')[:-3]} Preço: {last_auc[1]} Vol: {last_auc[2]} Flags: {last_auc[3]}")

# Contar ticks por minuto entre 18:24 e 18:35
from collections import Counter
minute_counts = Counter()
for r in rows:
    dt = norm.server_msc_to_utc_datetime(r[0]).astimezone(norm.market_tz)
    minute_counts[dt.strftime("%H:%M")] += 1

print("\nDistribuição de ticks por minuto no encerramento:")
for m in sorted(minute_counts.keys()):
    print(f"  Minuto {m} BRT: {minute_counts[m]} ticks")

cur.close()
conn.close()
