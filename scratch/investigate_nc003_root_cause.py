import sys
sys.path.insert(0, ".")
import json
import psycopg2
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import zoneinfo

out_dir = Path("scratch/audit_results_20261019")
with open(out_dir / "all_missing_ticks.json", "r", encoding="utf-8") as f:
    missing = json.load(f)

df_missing = pd.DataFrame(missing)

print("=== INVESTIGAÇÃO FORENSE DAS OMISSÕES DE TICKS (NC-003 E DEMAIS 18 JANELAS) ===")

conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur = conn.cursor()

# 1. Verificar se os ticks ausentes compartilham time_msc com ticks que FORAM capturados
shared_msc_count = 0
distinct_msc_count = 0
details_shared = []

for idx, r in df_missing.iterrows():
    msc = r['time_msc']
    cur.execute("SELECT COUNT(*), array_agg(price), array_agg(volume_real) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc = %s;", (msc,))
    res = cur.fetchone()
    count_in_pg = res[0]
    if count_in_pg > 0:
        shared_msc_count += 1
        details_shared.append({
            "time_msc": msc,
            "missing_price": r['price'],
            "missing_vol": r['volume_real'],
            "missing_side": r['trade_side'],
            "pg_ticks_count": count_in_pg,
            "pg_prices": res[1],
            "pg_vols": res[2]
        })
    else:
        distinct_msc_count += 1

print(f"Total de ticks ausentes analisados: {len(df_missing)}")
print(f"Ticks ausentes que compartilham time_msc com ticks JÁ gravados no PG: {shared_msc_count} ({shared_msc_count/len(df_missing)*100:.1f}%)")
print(f"Ticks ausentes com time_msc inteiramente ausente no PG: {distinct_msc_count} ({distinct_msc_count/len(df_missing)*100:.1f}%)")

if details_shared:
    print("\nExemplo de ticks ausentes com milissegundo compartilhado (mesmo time_msc):")
    df_shared = pd.DataFrame(details_shared)
    print(df_shared.head(10).to_string(index=False))

# 2. Verificar a proximidade temporal com o tick anterior capturado no PG
print("\nAnálise de delta temporal (ms) entre os ticks ausentes isolados e os ticks capturados adjacentes:")
gaps_before = []
gaps_after = []
for idx, r in df_missing[df_missing['time_msc'].isin([d['time_msc'] for d in details_shared]) == False].head(20).iterrows():
    msc = r['time_msc']
    cur.execute("SELECT MAX(time_msc) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc < %s;", (msc,))
    prev_msc = cur.fetchone()[0]
    cur.execute("SELECT MIN(time_msc) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc > %s;", (msc,))
    next_msc = cur.fetchone()[0]
    gaps_before.append(msc - prev_msc if prev_msc else None)
    gaps_after.append(next_msc - msc if next_msc else None)

print(f"Delta médio para o tick anterior capturado: {sum(filter(None, gaps_before))/len(list(filter(None, gaps_before))):.2f} ms")
print(f"Delta médio para o tick posterior capturado: {sum(filter(None, gaps_after))/len(list(filter(None, gaps_after))):.2f} ms")

# 3. Análise da NC-003 específica (14:01:21.300 BRT)
print("\n--- ANÁLISE ESPECÍFICA DA NC-003 (14:01:21.300 BRT) ---")
msc_nc003 = 1792418481300
cur.execute("SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, ingested_at_utc FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc BETWEEN %s AND %s ORDER BY time_msc, occurrence_ordinal;", (msc_nc003 - 50, msc_nc003 + 50))
adj_ticks = cur.fetchall()
print(f"Ticks capturados no PG na vizinhança de +/- 50ms de 14:01:21.300:")
for t in adj_ticks:
    print(f"  time_msc={t[0]} (Delta={t[0]-msc_nc003}ms) Price={t[1]} Vol={t[2]} Flags={t[3]} Side={t[4]} Ord={t[5]} Ingested={t[6]}")

cur.close()
conn.close()
