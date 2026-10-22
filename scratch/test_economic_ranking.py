import psycopg2
import pandas as pd

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"
conn = psycopg2.connect(SANDBOX_PG_URI)
cur = conn.cursor()

# Resetar provenance para o estado original do snapshot
cur.execute("UPDATE market_ticks_raw SET provenance = 'REALTIME_STREAM' WHERE provenance = 'DUPLICATE_TRANSPORT_ARTEFACT';")
conn.commit()

# Criar a View Analítica Canônica usando Identidade Econômica e desempate determinístico por ingested_at_utc
cur.execute("""
CREATE OR REPLACE VIEW v_market_ticks_economic_ranked AS
SELECT 
    symbol,
    time_msc,
    time_utc,
    price,
    volume_real,
    flags AS raw_flags,
    (flags & 104) AS identity_flags,
    trade_side,
    occurrence_ordinal AS raw_occurrence_ordinal,
    ROW_NUMBER() OVER (
        PARTITION BY symbol, time_msc, price, volume_real, trade_side 
        ORDER BY ingested_at_utc ASC, occurrence_ordinal ASC, tick_fingerprint ASC
    ) AS economic_ordinal,
    tick_fingerprint,
    provenance,
    ingested_at_utc
FROM market_ticks_raw;
""")
conn.commit()

print("View v_market_ticks_economic_ranked criada.")

# Testar na Janela 115 (Leilão)
cur.execute("""
    SELECT time_msc, price, volume_real, raw_flags, economic_ordinal, ingested_at_utc
    FROM v_market_ticks_economic_ranked
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 21:30:00+00' AND time_utc < '2026-10-19 21:35:00+00'
    ORDER BY time_msc, economic_ordinal;
""")
rows_115 = cur.fetchall()
df_115 = pd.DataFrame(rows_115, columns=['time_msc', 'price', 'vol', 'flags', 'econ_ord', 'ingested'])

print(f"Total registros na Janela 115: {len(df_115)}")
print(f"Registros com economic_ordinal == 1: {len(df_115[df_115['econ_ord'] == 1])} (Volume: {df_115[df_115['econ_ord'] == 1]['vol'].sum()})")
print(f"Registros com economic_ordinal > 1:  {len(df_115[df_115['econ_ord'] > 1])} (Volume: {df_115[df_115['econ_ord'] > 1]['vol'].sum()})")

cur.close()
conn.close()
