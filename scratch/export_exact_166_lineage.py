import psycopg2
import json

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"
conn = psycopg2.connect(SANDBOX_PG_URI)
cur = conn.cursor()

with open("scratch/audit_results_20261019/all_extra_ticks.json", "r", encoding="utf-8") as f:
    all_extra = json.load(f)

print(f"Total de excedentes em all_extra: {len(all_extra)}")

exact_lineage = []
for ex in all_extra:
    msc = ex['time_msc']
    price = ex['price']
    vol = ex['volume_real']
    ord_idx = ex['occurrence_ordinal']
    
    cur.execute("""
        SELECT tick_fingerprint, flags, occurrence_ordinal, ingested_at_utc
        FROM market_ticks_raw
        WHERE symbol = 'WINZ26' AND time_msc = %s AND price = %s AND volume_real = %s
        ORDER BY ingested_at_utc ASC;
    """, (msc, price, vol))
    rows = cur.fetchall()
    
    # Separar os que têm bit 256 (duplicata de transporte com flags 1336/1368) dos originais (flags 1080/1112)
    dup_candidates = [r for r in rows if r[1] & 256 != 0]
    orig_candidates = [r for r in rows if r[1] & 256 == 0]
    
    if not dup_candidates:
        # Se nenhum tiver bit 256 (ex: leilão ou sobreposição reconexão), o último ingerido é o duplicado
        if len(rows) >= 2:
            dup_candidates = [rows[-1]]
            orig_candidates = [rows[0]]
            
    for dc in dup_candidates:
        orig = orig_candidates[0] if orig_candidates else dc
        # Evitar duplicar na lista de lineage
        if any(item['duplicate_fingerprint'] == dc[0] for item in exact_lineage):
            continue
        exact_lineage.append({
            'duplicate_fingerprint': dc[0],
            'duplicate_time_msc': msc,
            'duplicate_price': price,
            'duplicate_volume_real': vol,
            'duplicate_flags': dc[1],
            'duplicate_ingested_at': dc[3].isoformat(),
            'original_fingerprint': orig[0],
            'original_flags': orig[1],
            'original_ingested_at': orig[3].isoformat(),
            'match_type': 'EXACT_ECONOMIC_MATCH'
        })

print(f"Total de registros de linhagem exatos mapeados: {len(exact_lineage)}")
assert len(exact_lineage) == 166, f"Esperado 166, obtido {len(exact_lineage)}"

with open("scratch/h001_exact_166_lineage.json", "w", encoding="utf-8") as f:
    json.dump(exact_lineage, f, indent=2)

print("Arquivo scratch/h001_exact_166_lineage.json salvo com sucesso.")
conn.close()
