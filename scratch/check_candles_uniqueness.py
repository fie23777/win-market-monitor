import psycopg2

conn = psycopg2.connect(host='127.0.0.1', port=5434, dbname='market_data', user='postgres', password='password')
cur = conn.cursor()
cur.execute("SELECT count(*), count(DISTINCT open_time_utc) FROM candles_m2 WHERE open_time_utc >= '2026-10-19 00:00:00+00';")
total, distinct = cur.fetchone()
print(f"Candles M2 Hoje: Total={total} | Únicos={distinct} | Duplicatas={total - distinct}")
conn.close()
