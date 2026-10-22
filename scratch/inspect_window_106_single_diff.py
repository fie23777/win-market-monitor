import sys
sys.path.insert(0, ".")
import psycopg2
from collections import Counter
from datetime import datetime, timezone
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader

conn = psycopg2.connect('postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox')
cur = conn.cursor()

cur.execute("""
    SELECT time_msc, price, volume_real, trade_side, occurrence_ordinal, tick_fingerprint, raw_flags
    FROM v_market_ticks_canonical 
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 20:45:00+00' AND time_utc < '2026-10-19 20:50:00+00'
    ORDER BY time_msc ASC, occurrence_ordinal ASC;
""")
pg_ticks = cur.fetchall()

norm = MT5TimeNormalizer()
r = MT5ReadOnlyReader(norm)
r.initialize()
raw_mt5 = r.fetch_ticks_range('WINZ26', datetime(2026, 10, 19, 20, 45, 0, tzinfo=timezone.utc), datetime(2026, 10, 19, 20, 50, 0, tzinfo=timezone.utc))
r.shutdown()

mt5_filtered = [t for t in raw_mt5 if 1792431900000 <= t['time_msc'] < 1792432200000 and ((t['flags'] & 32) or (t['flags'] & 64) or (t['flags'] & 8)) and t['last'] > 0 and t['volume_real'] > 0]

pg_c = Counter((x[0], x[1], float(x[2]), x[3], x[4]) for x in pg_ticks)

# Montar mt5 com ordinais
from collections import defaultdict
from src.domain.models import compute_tick_event_signature
mt5_groups = defaultdict(list)
for t in mt5_filtered:
    msc = t['time_msc']
    price = float(t['last'])
    vol = float(t['volume_real'])
    flags = int(t['flags'])
    side = 'BUY' if (flags & 32) and not (flags & 64) else ('SELL' if (flags & 64) and not (flags & 32) else 'CROSS')
    sig = compute_tick_event_signature(msc, price, vol, flags & ~256, side)
    mt5_groups[(msc, sig)].append((msc, price, vol, side))

mt5_items = []
for (msc, sig), insts in mt5_groups.items():
    for ord_idx, inst in enumerate(insts, start=1):
        mt5_items.append((inst[0], inst[1], inst[2], inst[3], ord_idx))

mt5_c = Counter(mt5_items)

diff_mt5 = mt5_c - pg_c
diff_pg = pg_c - mt5_c

print("No MT5 mas ausente no PG Canônico:", diff_mt5)
print("No PG Canônico mas ausente no MT5:", diff_pg)

cur.close()
conn.close()
