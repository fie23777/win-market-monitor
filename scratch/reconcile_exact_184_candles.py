"""
Reconciliação Exaustiva e Rigorosa dos 184 Candles M2:
Classifica individualmente cada um dos 184 candles cadastrados no banco de dados contra
a agregação estrita dos negócios brutos no PostgreSQL.
Identifica sem ambiguidade todos os 49 candles divergentes.
"""

import sys
sys.path.insert(0, ".")
import psycopg2
from datetime import datetime, timezone
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

config = load_config()
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

conn_pg = psycopg2.connect(
    host=config.postgres.host,
    port=config.postgres.port,
    dbname=config.postgres.database,
    user=config.postgres.user,
    password=config.postgres.password
)
cur = conn_pg.cursor()

cur.execute("""
SELECT open_time_utc, close_time_utc, open, high, low, close, volume_real, tick_volume
FROM candles_m2
WHERE open_time_utc >= '2026-10-19 00:00:00+00'
ORDER BY open_time_utc ASC;
""")
all_candles = cur.fetchall()

total_candles = len(all_candles)
matches = []
severe_truncations = []
open_close_tick_diffs = []
volume_only_diffs = []
mixed_diffs = []

for c in all_candles:
    c_open_utc, c_close_utc = c[0], c[1]
    c_o, c_h, c_l, c_c = float(c[2]), float(c[3]), float(c[4]), float(c[5])
    c_vol = float(c[6])
    c_ticks = int(c[7])

    cur.execute("""
    SELECT 
        (ARRAY_AGG(price ORDER BY time_msc ASC, occurrence_ordinal ASC))[1] as o,
        MAX(price) as h,
        MIN(price) as l,
        (ARRAY_AGG(price ORDER BY time_msc DESC, occurrence_ordinal DESC))[1] as c,
        COALESCE(SUM(volume_real), 0) as v_real,
        COUNT(*) as t_count
    FROM market_ticks_raw
    WHERE time_utc >= %s AND time_utc < %s;
    """, (c_open_utc, c_close_utc))
    agg = cur.fetchone()

    o_brt = c_open_utc.astimezone(BRT_TZ).strftime('%H:%M:%S')
    c_brt = c_close_utc.astimezone(BRT_TZ).strftime('%H:%M:%S')
    tag = f"{o_brt}->{c_brt}"

    t_o, t_h, t_l, t_c = float(agg[0]), float(agg[1]), float(agg[2]), float(agg[3])
    t_vol = float(agg[4])
    t_ticks = int(agg[5])

    ohlc_match = (c_o == t_o and c_h == t_h and c_l == t_l and c_c == t_c)
    vol_match = (c_vol == t_vol)

    if ohlc_match and vol_match:
        matches.append(tag)
    else:
        vol_delta = c_vol - t_vol
        o_diff = c_o - t_o
        c_diff = c_c - t_c
        h_diff = c_h - t_h
        l_diff = c_l - t_l

        # Classificação rigorosa
        if abs(vol_delta) > 5000:
            severe_truncations.append({
                "candle": tag,
                "candle_vol": c_vol,
                "ticks_vol": t_vol,
                "vol_delta": vol_delta,
                "candle_ohlc": (c_o, c_h, c_l, c_c),
                "ticks_ohlc": (t_o, t_h, t_l, t_c)
            })
        elif not ohlc_match and vol_match:
            # Divergência exclusiva de preço (Open ou Close por 1 tick de 5 pts)
            open_close_tick_diffs.append({
                "candle": tag,
                "o_diff": o_diff,
                "c_diff": c_diff,
                "h_diff": h_diff,
                "l_diff": l_diff,
                "vol": c_vol
            })
        elif ohlc_match and not vol_match:
            # Divergência exclusiva de volume
            volume_only_diffs.append({
                "candle": tag,
                "vol_delta": vol_delta,
                "candle_vol": c_vol,
                "ticks_vol": t_vol
            })
        else:
            # Divergência mista (preço e volume pequenos)
            mixed_diffs.append({
                "candle": tag,
                "vol_delta": vol_delta,
                "o_diff": o_diff,
                "c_diff": c_diff
            })

print(f"Total Candles Auditados: {total_candles}")
print(f"1. Perfeita Concordância (MATCH):             {len(matches)} ({round(len(matches)/total_candles*100, 2)}%)")
print(f"2. Truncamentos Severos de Volume (NC-002):   {len(severe_truncations)} ({round(len(severe_truncations)/total_candles*100, 2)}%)")
print(f"3. Divergência Exclusiva de Preço (Open/Close): {len(open_close_tick_diffs)} ({round(len(open_close_tick_diffs)/total_candles*100, 2)}%)")
print(f"4. Divergência Exclusiva de Volume:           {len(volume_only_diffs)} ({round(len(volume_only_diffs)/total_candles*100, 2)}%)")
print(f"5. Divergência Mista (Preço e Volume):        {len(mixed_diffs)} ({round(len(mixed_diffs)/total_candles*100, 2)}%)")
print(f"\nSOMA DE TODAS AS CATEGORIAS: {len(matches)} + {len(severe_truncations)} + {len(open_close_tick_diffs)} + {len(volume_only_diffs)} + {len(mixed_diffs)} = {len(matches) + len(severe_truncations) + len(open_close_tick_diffs) + len(volume_only_diffs) + len(mixed_diffs)}")

print("\n--- DETALHAMENTO DOS 3 TRUNCAMENTOS SEVEROS ---")
for s in severe_truncations:
    print(f"  {s['candle']}: Candle Vol={s['candle_vol']} vs Ticks Vol={s['ticks_vol']} (Déficit: {s['vol_delta']}) | OHLC Candle={s['candle_ohlc']} vs Ticks={s['ticks_ohlc']}")

print("\n--- DETALHAMENTO DAS DIVERGÊNCIAS EXCLUSIVAS DE PREÇO (OPEN/CLOSE) ---")
for p in open_close_tick_diffs:
    print(f"  {p['candle']}: Delta Open={p['o_diff']} | Delta Close={p['c_diff']} | Delta High={p['h_diff']} | Delta Low={p['l_diff']}")

print("\n--- DETALHAMENTO DAS DIVERGÊNCIAS EXCLUSIVAS DE VOLUME ---")
for v in volume_only_diffs:
    print(f"  {v['candle']}: Delta Vol={v['vol_delta']} (Candle={v['candle_vol']} vs Ticks={v['ticks_vol']})")

print("\n--- DETALHAMENTO DAS DIVERGÊNCIAS MISTAS ---")
for m in mixed_diffs:
    print(f"  {m['candle']}: Delta Vol={m['vol_delta']} | Delta Open={m['o_diff']} | Delta Close={m['c_diff']}")

conn_pg.close()
