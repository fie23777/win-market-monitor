import psycopg2
import pandas as pd

conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur = conn.cursor()

cur.execute("""
    SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, ingested_at_utc, tick_fingerprint
    FROM market_ticks_raw 
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 20:45:00+00' AND time_utc < '2026-10-19 20:50:00+00' 
    ORDER BY time_msc ASC, occurrence_ordinal ASC;
""")
rows = cur.fetchall()
df = pd.DataFrame(rows, columns=['time_msc', 'price', 'volume_real', 'flags', 'trade_side', 'occurrence_ordinal', 'ingested_at_utc', 'tick_fingerprint'])

print(f"Total registros na Janela 106: {len(df)}")
print("\nContagem por flags na Janela 106:")
print(df['flags'].value_counts())

print("\nContagem por lote de ingested_at_utc na Janela 106:")
print(df['ingested_at_utc'].value_counts().head(10))

# Verificar se há registros com flags 1336 / 1368 (com bit 256)
df_256 = df[df['flags'] & 256 != 0]
print(f"\nTotal com bit 256 na Janela 106: {len(df_256)}")
if not df_256.empty:
    print(df_256[['time_msc', 'price', 'volume_real', 'flags', 'ingested_at_utc']].head(10))

# Analisar o intervalo 17:45:19 (time_msc correspondente)
df['time_brt'] = pd.to_datetime(df['time_msc'], unit='ms').dt.tz_localize('UTC') # time_msc civil
print("\nTicks em torno das 17:45:19 BRT:")
sub_19 = df[df['time_msc'].between(1792431918000, 1792431920000)]
print(f"Total de ticks entre 17:45:18 e 17:45:20: {len(sub_19)}")
print("Ingested at desses ticks:")
print(sub_19['ingested_at_utc'].value_counts())
print("Flags desses ticks:")
print(sub_19['flags'].value_counts())

cur.close()
conn.close()
