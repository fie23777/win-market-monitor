import sys
sys.path.insert(0, ".")
import psycopg2
import json
import pandas as pd

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"

print("=== ETAPA 1: CRIAÇÃO DA TABELA DE LINHAGEM E VIEW ANALÍTICA CANÔNICA NO SANDBOX CLONE ===")

conn = psycopg2.connect(SANDBOX_PG_URI)
cur = conn.cursor()

# 1. Criar tabela de linhagem de duplicatas
cur.execute("""
CREATE TABLE IF NOT EXISTS duplicate_lineage (
    duplicate_fingerprint VARCHAR(64) PRIMARY KEY,
    duplicate_time_msc BIGINT NOT NULL,
    duplicate_price DOUBLE PRECISION NOT NULL,
    duplicate_volume_real DOUBLE PRECISION NOT NULL,
    duplicate_flags INTEGER NOT NULL,
    duplicate_ingested_at TIMESTAMPTZ NOT NULL,
    original_fingerprint VARCHAR(64) NOT NULL,
    original_flags INTEGER NOT NULL,
    original_ingested_at TIMESTAMPTZ NOT NULL,
    match_type VARCHAR(32) NOT NULL,
    linked_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
""")
conn.commit()
print("Tabela duplicate_lineage criada.")

# 2. Carregar os 166 excedentes mapeados
with open("scratch/audit_results_20261019/all_extra_ticks.json", "r", encoding="utf-8") as f:
    all_extra = json.load(f)

print(f"Total de registros excedentes a mapear: {len(all_extra)}")

linked_count = 0
for ex in all_extra:
    msc = ex['time_msc']
    price = ex['price']
    vol = ex['volume_real']
    
    # Buscar todos os registros com mesma identidade econômica no Sandbox Clone
    cur.execute("""
        SELECT tick_fingerprint, flags, ingested_at_utc, occurrence_ordinal
        FROM market_ticks_raw
        WHERE symbol = 'WINZ26' AND time_msc = %s AND price = %s AND volume_real = %s
        ORDER BY ingested_at_utc ASC;
    """, (msc, price, vol))
    rows = cur.fetchall()
    
    # Deve haver pelo menos 2 registros (o original e a duplicata)
    if len(rows) >= 2:
        orig = rows[0] # Primeiro ingerido (original em tempo real)
        dup = rows[-1] # Último ingerido (duplicata histórica com bit 256)
        
        cur.execute("""
            INSERT INTO duplicate_lineage (
                duplicate_fingerprint, duplicate_time_msc, duplicate_price, duplicate_volume_real,
                duplicate_flags, duplicate_ingested_at, original_fingerprint, original_flags,
                original_ingested_at, match_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'EXACT_ECONOMIC_MATCH')
            ON CONFLICT (duplicate_fingerprint) DO NOTHING;
        """, (
            dup[0], msc, price, vol, dup[1], dup[2],
            orig[0], orig[1], orig[2]
        ))
        
        # Marcar provenance do registro duplicado físico no clone
        cur.execute("""
            UPDATE market_ticks_raw
            SET provenance = 'DUPLICATE_TRANSPORT_ARTEFACT'
            WHERE symbol = 'WINZ26' AND tick_fingerprint = %s;
        """, (dup[0],))
        linked_count += 1

conn.commit()
print(f"Total de duplicatas vinculadas com linhagem comprovada: {linked_count} / {len(all_extra)}")

# 3. Criar a Visão Analítica Canônica Dedupicada v_market_ticks_canonical
cur.execute("""
CREATE OR REPLACE VIEW v_market_ticks_canonical AS
SELECT 
    symbol,
    time_msc,
    time_utc,
    price,
    volume_real,
    trade_side,
    occurrence_ordinal,
    flags AS raw_flags,
    (flags & 104) AS identity_flags, -- Isola TICK_FLAG_BUY(32), TICK_FLAG_SELL(64), TICK_FLAG_LAST(8)
    tick_fingerprint,
    provenance,
    ingested_at_utc
FROM market_ticks_raw
WHERE provenance != 'DUPLICATE_TRANSPORT_ARTEFACT';
""")
conn.commit()
print("View v_market_ticks_canonical criada com sucesso.")

# Validar contagem da View no Sandbox Clone
cur.execute("SELECT COUNT(*), SUM(volume_real) FROM v_market_ticks_canonical WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 12:00:00+00' AND time_utc < '2026-10-19 21:35:00+00';")
v_count, v_vol = cur.fetchone()
print(f"Visão Canônica no Sandbox Clone (09:00 às 18:35 BRT):")
print(f"  Contagem de Ticks Canônicos: {v_count} (Esperado: 2.093.221 - 166 = 2.093.055)")
print(f"  Volume Canônico Contratos:   {v_vol:,.1f} (Esperado: 19.838.713 - 26.070 = 19.812.643,0)")

cur.close()
conn.close()
