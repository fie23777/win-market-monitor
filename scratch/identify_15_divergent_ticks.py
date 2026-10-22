import sys
sys.path.insert(0, ".")
import sqlite3
import psycopg2
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

normalizer = MT5TimeNormalizer()

spool_path = Path("F:/win-market-monitor-backup/session_closing_snapshot_20261019_214726/spool.db")

print("=== IDENTIFICAÇÃO DOS 15 REGISTROS PRESENTES NO POSTGRESQL E AUSENTES NO SQLITE ===")

# Conectar SQLite
conn_sq = sqlite3.connect(f"file:{spool_path}?mode=ro", uri=True)
cur_sq = conn_sq.cursor()

# Extrair todos os fingerprints do SQLite
print("Carregando fingerprints do SQLite Spool...")
cur_sq.execute("SELECT tick_fingerprint FROM spool_ticks;")
sq_fps = set(row[0] for row in cur_sq.fetchall())
print(f"Total fingerprints únicos no SQLite: {len(sq_fps)}")

cur_sq.close()
conn_sq.close()

# Conectar PostgreSQL (somente leitura)
conn_pg = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur_pg = conn_pg.cursor()

print("Consultando registros do PostgreSQL...")
cur_pg.execute("""
    SELECT symbol, time_msc, tick_fingerprint, time_utc, price, volume_real, flags, trade_side, occurrence_ordinal, provenance, ingested_at_utc
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26'
    ORDER BY time_msc ASC, occurrence_ordinal ASC;
""")

cols = [desc[0] for desc in cur_pg.description]
all_pg_rows = cur_pg.fetchall()
print(f"Total registros no PG: {len(all_pg_rows)}")

divergent_rows = []
for row in all_pg_rows:
    fp = row[2]
    if fp not in sq_fps:
        divergent_rows.append(row)

print(f"\nTotal de registros divergentes encontrados: {len(divergent_rows)}")

df_div = pd.DataFrame(divergent_rows, columns=cols)

for idx, r in df_div.iterrows():
    msc = r['time_msc']
    dt_brt = normalizer.server_msc_to_utc_datetime(msc).astimezone(normalizer.market_tz)
    print(f"\n--- Divergência #{idx+1} ---")
    print(f"Fingerprint:        {r['tick_fingerprint']}")
    print(f"time_msc:          {r['time_msc']}")
    print(f"Data/Hora BRT:     {dt_brt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} BRT")
    print(f"time_utc:          {r['time_utc']}")
    print(f"Preço:             {r['price']}")
    print(f"Volume Real:       {r['volume_real']}")
    print(f"Flags:             {r['flags']}")
    print(f"Trade Side:        {r['trade_side']}")
    print(f"Occurrence Ord:    {r['occurrence_ordinal']}")
    print(f"Provenance:        {r['provenance']}")
    print(f"Ingested at UTC:   {r['ingested_at_utc']}")

cur_pg.close()
conn_pg.close()
