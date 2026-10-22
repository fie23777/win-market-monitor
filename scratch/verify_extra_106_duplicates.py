import psycopg2
import json

conn = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur = conn.cursor()

with open("scratch/audit_results_20261019/all_extra_ticks.json") as f:
    extra_106 = [x for x in json.load(f) if x['window'] == 106]

confirmed_dups = 0
examples = []

for x in extra_106:
    cur.execute("""
        SELECT COUNT(*), array_agg(flags), array_agg(provenance), array_agg(ingested_at_utc)
        FROM market_ticks_raw 
        WHERE symbol = %s AND time_msc = %s AND price = %s AND volume_real = %s;
    """, ('WINZ26', x['time_msc'], x['price'], x['volume_real']))
    c, fl, pr, ing = cur.fetchone()
    if c > 1:
        confirmed_dups += 1
        if len(examples) < 5:
            examples.append({
                "time_msc": x['time_msc'],
                "price": x['price'],
                "vol": x['volume_real'],
                "count": c,
                "flags": fl,
                "ingested": [str(t) for t in ing]
            })

print(f"Total de registros analisados da Janela 106: {len(extra_106)}")
print(f"Quantos possuem duplicata comprovada no PostgreSQL: {confirmed_dups} / {len(extra_106)} ({confirmed_dups/len(extra_106)*100:.1f}%)")

print("\nExemplos de duplicatas comprovadas na Janela 106:")
for ex in examples:
    print(f"  time_msc={ex['time_msc']} Preço={ex['price']} Vol={ex['vol']} Qtd={ex['count']} Flags={ex['flags']}")
    print(f"    Ingested em: {ex['ingested']}")

cur.close()
conn.close()
