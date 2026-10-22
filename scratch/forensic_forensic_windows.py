import sys
sys.path.insert(0, ".")
import json
import psycopg2
from pathlib import Path
from collections import Counter
import pandas as pd
from datetime import datetime, timezone
import zoneinfo

from src.connection.mt5_time_normalizer import MT5TimeNormalizer

normalizer = MT5TimeNormalizer()
tz_brt = zoneinfo.ZoneInfo("America/Sao_Paulo")

print("=== INVESTIGAÇÃO DETALHADA DA JANELA 115 (LEILÃO 18:30-18:35) ===")

conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur = conn.cursor()

# Buscar todos os ticks da Janela 115 no PG
cur.execute("""
    SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, tick_fingerprint, provenance, ingested_at_utc
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 21:30:00+00' AND time_utc < '2026-10-19 21:35:00+00'
    ORDER BY time_msc ASC, occurrence_ordinal ASC, price ASC, volume_real ASC;
""")
rows_115 = cur.fetchall()
df_115 = pd.DataFrame(rows_115, columns=['time_msc', 'price', 'volume_real', 'flags', 'trade_side', 'occurrence_ordinal', 'tick_fingerprint', 'provenance', 'ingested_at_utc'])
print(f"Total de registros no PG na Janela 115: {len(df_115)}")
print(f"Volume Real total no PG: {df_115['volume_real'].sum()}")
print(f"Valores únicos de time_msc na Janela 115:")
print(df_115['time_msc'].value_counts())

# Verificar duplicidades de trades (mesmo time_msc, price, volume_real)
grouped_115 = df_115.groupby(['time_msc', 'price', 'volume_real', 'trade_side']).size().reset_index(name='count')
print(f"\nAgrupamento por (time_msc, price, volume_real, trade_side) na Janela 115:")
print(grouped_115.to_string())

# Verificar proveniência e ingested_at_utc na Janela 115
print("\nProveniência e ingested_at_utc na Janela 115:")
print(df_115[['provenance', 'ingested_at_utc', 'occurrence_ordinal', 'flags']].drop_duplicates().to_string())

print("\n=== INVESTIGAÇÃO DETALHADA DA JANELA 106 (17:45-17:50) ===")
cur.execute("""
    SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, tick_fingerprint, provenance, ingested_at_utc
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 20:45:00+00' AND time_utc < '2026-10-19 20:50:00+00'
    ORDER BY time_msc ASC, occurrence_ordinal ASC;
""")
rows_106 = cur.fetchall()
df_106 = pd.DataFrame(rows_106, columns=['time_msc', 'price', 'volume_real', 'flags', 'trade_side', 'occurrence_ordinal', 'tick_fingerprint', 'provenance', 'ingested_at_utc'])
print(f"Total de registros no PG na Janela 106: {len(df_106)}")
print(f"Volume Real total no PG: {df_106['volume_real'].sum()}")

# Verificar se há registros RECOVERED na Janela 106
print("\nDistribuição por proveniência na Janela 106:")
print(df_106['provenance'].value_counts())

cur.close()
conn.close()
