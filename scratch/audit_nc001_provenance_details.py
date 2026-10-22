import psycopg2
import pandas as pd

conn = psycopg2.connect("postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox")
cur = conn.cursor()

# 1. Contagem por provenance no sandbox
cur.execute("""
    SELECT provenance, count(*), sum(volume_real), min(time_utc), max(time_utc), min(ingested_at_utc), max(ingested_at_utc)
    FROM market_ticks_raw
    GROUP BY provenance
    ORDER BY count(*) DESC;
""")
rows = cur.fetchall()
print("Distribuição de Ticks por Provenance:")
for r in rows:
    print(f"  {r[0]:<30} | count={r[1]:>8} | vol={r[2]:>12,.1f} | time_utc: {r[3]} a {r[4]} | ingested_at_utc: {r[5]} a {r[6]}")

# 2. Investigação aprofundada dos 44.899 de RECOVERED_GAP_MT5 (NC-001)
cur.execute("""
    SELECT count(*), min(time_msc), max(time_msc), min(time_utc), max(time_utc),
           count(DISTINCT time_msc), sum(volume_real)
    FROM market_ticks_raw
    WHERE provenance = 'RECOVERED_GAP_MT5';
""")
nc1 = cur.fetchone()
print(f"\nNC-001 RECOVERED_GAP_MT5:")
print(f"  Total: {nc1[0]} ticks, Volume: {nc1[6]:,.1f}")
print(f"  Range time_utc: {nc1[3]} até {nc1[4]}")
print(f"  Range time_msc: {nc1[1]} até {nc1[2]}")
print(f"  Timestamps distintos: {nc1[5]}")

# Verificar se existe algum tick STREAMING_LIVE dentro dessa mesma janela temporal (14:07 a 14:23 BRT = 17:07 a 17:23 UTC)
cur.execute("""
    SELECT provenance, count(*)
    FROM market_ticks_raw
    WHERE time_utc >= %s AND time_utc <= %s
    GROUP BY provenance;
""", (nc1[3], nc1[4]))
print(f"\nProvenances dentro da janela do Blackout ({nc1[3]} a {nc1[4]}):")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} ticks")

# Verificar no spool.db original ou sandbox se esses ticks existem
conn.close()
