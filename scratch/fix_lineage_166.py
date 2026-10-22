import psycopg2
import json
import pandas as pd

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"
conn = psycopg2.connect(SANDBOX_PG_URI)
cur = conn.cursor()

with open("scratch/audit_results_20261019/all_extra_ticks.json", "r", encoding="utf-8") as f:
    all_extra = json.load(f)

# Buscar todas as linhas duplicadas marcadas
cur.execute("SELECT duplicate_fingerprint FROM duplicate_lineage;")
linked_fps = set(r[0] for r in cur.fetchall())

print(f"Total em all_extra: {len(all_extra)}")
print(f"Total em duplicate_lineage: {len(linked_fps)}")

# Identificar os excedentes que não entraram em duplicate_lineage
missing_lineage = []
for ex in all_extra:
    msc = ex['time_msc']
    price = ex['price']
    vol = ex['volume_real']
    ord_idx = ex['occurrence_ordinal']
    cur.execute("""
        SELECT tick_fingerprint, flags, occurrence_ordinal, ingested_at_utc
        FROM market_ticks_raw
        WHERE symbol = 'WINZ26' AND time_msc = %s AND price = %s AND volume_real = %s;
    """, (msc, price, vol))
    rows = cur.fetchall()
    # Identificar quais fingerprints correspondem aos que têm flags 1336 / 1368 (com bit 256)
    dup_candidates = [r for r in rows if r[1] & 256 != 0]
    orig_candidates = [r for r in rows if r[1] & 256 == 0]
    
    for dc in dup_candidates:
        if dc[0] not in linked_fps:
            orig = orig_candidates[0] if orig_candidates else None
            missing_lineage.append((dc, orig, msc, price, vol))

print(f"Excedentes pendentes de vinculação: {len(missing_lineage)}")
for dc, orig, msc, price, vol in missing_lineage:
    orig_fp = orig[0] if orig else "UNKNOWN"
    orig_flags = orig[1] if orig else 0
    orig_ing = orig[3] if orig else dc[3]
    cur.execute("""
        INSERT INTO duplicate_lineage (
            duplicate_fingerprint, duplicate_time_msc, duplicate_price, duplicate_volume_real,
            duplicate_flags, duplicate_ingested_at, original_fingerprint, original_flags,
            original_ingested_at, match_type
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'EXACT_ECONOMIC_MATCH')
        ON CONFLICT (duplicate_fingerprint) DO NOTHING;
    """, (dc[0], msc, price, vol, dc[1], dc[3], orig_fp, orig_flags, orig_ing))
    cur.execute("""
        UPDATE market_ticks_raw
        SET provenance = 'DUPLICATE_TRANSPORT_ARTEFACT'
        WHERE symbol = 'WINZ26' AND tick_fingerprint = %s;
    """, (dc[0],))

conn.commit()

cur.execute("SELECT COUNT(*) FROM market_ticks_raw WHERE provenance = 'DUPLICATE_TRANSPORT_ARTEFACT';")
dups_total = cur.fetchone()[0]
print(f"Total final de DUPLICATE_TRANSPORT_ARTEFACT marcados: {dups_total} (Esperado: 166)")

# Recontar na visão canônica
cur.execute("""
SELECT COUNT(*), SUM(volume_real) 
FROM v_market_ticks_canonical 
WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 12:00:00+00' AND time_utc < '2026-10-19 21:35:00+00';
""")
v_count, v_vol = cur.fetchone()
print(f"\nVisão Canônica Recalculada no Sandbox Clone:")
print(f"  Contagem de Ticks Canônicos: {v_count} (Esperado: 2.093.055)")
print(f"  Volume Canônico Contratos:   {v_vol:,.1f} (Esperado: 19.812.643,0)")

cur.close()
conn.close()
