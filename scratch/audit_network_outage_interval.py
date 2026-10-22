import sys
sys.path.insert(0, ".")
import psycopg2
from datetime import datetime, timezone
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

config = load_config()
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

conn_pg = psycopg2.connect(
    host=config.postgres.host,
    port=config.postgres.port,
    dbname=config.postgres.database,
    user=config.postgres.user,
    password=config.postgres.password
)
cur = conn_pg.cursor()

# Consultar ticks entre 14:05 BRT e 14:25 BRT (17:05 UTC a 17:25 UTC)
cur.execute("""
SELECT 
    count(*) as total_ticks,
    count(*) FILTER (WHERE provenance = 'RECOVERED_HISTORICAL_MT5') as historical_ticks,
    count(*) FILTER (WHERE provenance = 'REALTIME_STREAM') as realtime_ticks,
    min(time_utc) as min_time,
    max(time_utc) as max_time,
    min(ingested_at_utc) as min_ingested,
    max(ingested_at_utc) as max_ingested
FROM market_ticks_raw
WHERE time_utc >= '2026-10-19 17:05:00+00' AND time_utc <= '2026-10-19 17:25:00+00';
""")
row = cur.fetchone()

print("=== AUDITORIA DO INTERVALO DE OSCILAÇÃO DE REDE (14:05 A 14:25 BRT) ===")
print("Total ticks no intervalo de mercado:", row[0])
print("Classificados como Histórico:", row[1])
print("Classificados como Streaming:", row[2])
if row[3]:
    print("Min Market Time BRT:", row[3].astimezone(BRT_TZ).strftime("%H:%M:%S.%f")[:-3])
    print("Max Market Time BRT:", row[4].astimezone(BRT_TZ).strftime("%H:%M:%S.%f")[:-3])
    print("Min Ingested Time BRT:", row[5].astimezone(BRT_TZ).strftime("%H:%M:%S.%f")[:-3])
    print("Max Ingested Time BRT:", row[6].astimezone(BRT_TZ).strftime("%H:%M:%S.%f")[:-3])

# Checagem de lacunas temporais entre negócios consecutivos
cur.execute("""
WITH ordered AS (
    SELECT time_utc, LAG(time_utc) OVER (ORDER BY time_utc, occurrence_ordinal) as prev_time
    FROM market_ticks_raw
    WHERE time_utc >= '2026-10-19 17:05:00+00' AND time_utc <= '2026-10-19 17:25:00+00'
)
SELECT time_utc, prev_time, EXTRACT(EPOCH FROM (time_utc - prev_time)) as gap_sec
FROM ordered
WHERE EXTRACT(EPOCH FROM (time_utc - prev_time)) > 30.0
ORDER BY gap_sec DESC
LIMIT 10;
""")
gaps = cur.fetchall()
print("\nMaiores intervalos entre negócios consecutivos (> 30s):")
if gaps:
    for g in gaps:
        t_curr = g[0].astimezone(BRT_TZ).strftime("%H:%M:%S")
        t_prev = g[1].astimezone(BRT_TZ).strftime("%H:%M:%S")
        print(f"  De {t_prev} até {t_curr} (duracao: {round(float(g[2]), 1)}s)")
else:
    print("  Nenhum intervalo > 30s encontrado! O fluxo de negócios foi integralmente recuperado sem gaps!")

conn_pg.close()
