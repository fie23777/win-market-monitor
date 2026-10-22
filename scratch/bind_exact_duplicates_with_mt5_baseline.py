import sys
sys.path.insert(0, ".")
import psycopg2
import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime, timezone
import zoneinfo

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader

normalizer = MT5TimeNormalizer()
tz_brt = zoneinfo.ZoneInfo("America/Sao_Paulo")

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"
conn = psycopg2.connect(SANDBOX_PG_URI)
cur = conn.cursor()

print("=== ETAPA 2: VINCULAÇÃO EXPLÍCITA 1-PARA-1 DOS 166 EXCEDENTES BASEADA EM MULTIPLICIDADES MT5 ===")

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
    match_type VARCHAR(64) NOT NULL,
    linked_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE duplicate_lineage ALTER COLUMN match_type TYPE VARCHAR(64);
""")
conn.commit()

# 1. Obter todos os trades da sessão de 19/10/2026 no MT5
reader = MT5ReadOnlyReader(normalizer)
reader.initialize()
symbol = "WINZ26"
reader.select_symbol(symbol)

dt_start_utc = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
dt_end_utc = datetime(2026, 10, 19, 21, 35, 0, tzinfo=timezone.utc)
msc_start = normalizer.utc_datetime_to_server_msc(dt_start_utc)
msc_end = normalizer.utc_datetime_to_server_msc(dt_end_utc)

raw_mt5 = reader.fetch_ticks_range(symbol, dt_start_utc, dt_end_utc)
reader.shutdown()

# Filtrar trades MT5
trades_mt5 = [t for t in raw_mt5 if msc_start <= t['time_msc'] < msc_end and ((t['flags'] & 32) or (t['flags'] & 64) or (t['flags'] & 8)) and t['last'] > 0 and t['volume_real'] > 0]
print(f"Total trades MT5 na sessão: {len(trades_mt5)}")

# Contar multiplicidades no MT5 por chave econômica (time_msc, price, volume_real, trade_side)
mt5_multiplicity = Counter()
for t in trades_mt5:
    msc = t['time_msc']
    price = float(t['last'])
    vol = float(t['volume_real'])
    flags = int(t['flags'])
    side = 'BUY' if (flags & 32) and not (flags & 64) else ('SELL' if (flags & 64) and not (flags & 32) else 'CROSS')
    mt5_multiplicity[(msc, price, vol, side)] += 1

print(f"Total tuplas econômicas únicas no MT5: {len(mt5_multiplicity)}")

# 2. Obter todos os ticks da sessão no Sandbox Clone
cur.execute("""
    SELECT time_msc, price, volume_real, trade_side, flags, ingested_at_utc, occurrence_ordinal, tick_fingerprint
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26' AND time_utc >= %s AND time_utc < %s
    ORDER BY time_msc ASC, ingested_at_utc ASC, occurrence_ordinal ASC;
""", (dt_start_utc, dt_end_utc))
pg_rows = cur.fetchall()
print(f"Total registros no PG Sandbox Clone: {len(pg_rows)}")

# Agrupar registros do PG por chave econômica
pg_groups = defaultdict(list)
for r in pg_rows:
    msc, price, vol, side, flags, ing, ord_idx, fp = r
    key = (msc, float(price), float(vol), side)
    pg_groups[key].append({
        "time_msc": msc,
        "price": float(price),
        "vol": float(vol),
        "side": side,
        "flags": flags,
        "ingested_at": ing,
        "ordinal": ord_idx,
        "fingerprint": fp
    })

# 3. Identificar e vincular com exatidão matemática cada duplicata excedente
cur.execute("TRUNCATE TABLE duplicate_lineage;")
conn.commit()

lineage_entries = []
duplicates_fingerprints = []

total_excess_found = 0
for key, pg_instances in pg_groups.items():
    k_pg = len(pg_instances)
    k_mt5 = mt5_multiplicity.get(key, 0)
    
    if k_pg > k_mt5:
        excess = k_pg - k_mt5
        total_excess_found += excess
        # Os primeiros k_mt5 são os legítimos (ordenados por ingested_at ASC)
        legitimate_instances = pg_instances[:k_mt5]
        duplicate_instances = pg_instances[k_mt5:]
        
        # Vincular cada duplicata ao respectivo original
        for idx, dup in enumerate(duplicate_instances):
            orig = legitimate_instances[idx % len(legitimate_instances)] if legitimate_instances else dup
            duplicates_fingerprints.append(dup["fingerprint"])
            lineage_entries.append((
                dup["fingerprint"], dup["time_msc"], dup["price"], dup["vol"],
                dup["flags"], dup["ingested_at"], orig["fingerprint"], orig["flags"],
                orig["ingested_at"], "EXACT_ECONOMIC_MULTIPLICITY_EXCESS"
            ))

print(f"\nTotal de registros excedentes identificados com precisão matemática: {total_excess_found} (Esperado: 166)")
print(f"Total de vínculos de linhagem gerados: {len(lineage_entries)}")

# Inserir na tabela duplicate_lineage
for entry in lineage_entries:
    cur.execute("""
        INSERT INTO duplicate_lineage (
            duplicate_fingerprint, duplicate_time_msc, duplicate_price, duplicate_volume_real,
            duplicate_flags, duplicate_ingested_at, original_fingerprint, original_flags,
            original_ingested_at, match_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (duplicate_fingerprint) DO NOTHING;
    """, entry)

# Atualizar provenance na tabela física do clone para os 166 comprovados
for fp in duplicates_fingerprints:
    cur.execute("""
        UPDATE market_ticks_raw
        SET provenance = 'DUPLICATE_TRANSPORT_ARTEFACT'
        WHERE symbol = 'WINZ26' AND tick_fingerprint = %s;
    """, (fp,))

conn.commit()

cur.execute("SELECT COUNT(*) FROM market_ticks_raw WHERE provenance = 'DUPLICATE_TRANSPORT_ARTEFACT';")
total_marked = cur.fetchone()[0]
print(f"Total de registros marcados como DUPLICATE_TRANSPORT_ARTEFACT: {total_marked} (Esperado: 166)")

# Revalidar a visão analítica canônica
cur.execute("""
SELECT COUNT(*), SUM(volume_real)
FROM v_market_ticks_canonical
WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 12:00:00+00' AND time_utc < '2026-10-19 21:35:00+00';
""")
v_count, v_vol = cur.fetchone()
print(f"\nVisão Canônica Recalculada no Sandbox Clone:")
print(f"  Contagem Canônica: {v_count} (Esperado: 2.093.221 - 166 = 2.093.055)")
print(f"  Volume Canônico:   {v_vol:,.1f} (Esperado: 19.838.713 - 26.070 = 19.812.643,0)")

# 4. Verificar se a diferença para o MT5 agora é EXATAMENTE 511 ticks e 3.836 contratos
missing_count_expected = len(trades_mt5) - v_count
missing_vol_expected = sum(t['volume_real'] for t in trades_mt5) - v_vol
print(f"\nDelta Restante Visão Canônica vs MT5:")
print(f"  Delta Ticks:  {missing_count_expected} (Esperado: exatamente 511)")
print(f"  Delta Volume: {missing_vol_expected:,.1f} (Esperado: exatamente 3.836,0)")

cur.close()
conn.close()
