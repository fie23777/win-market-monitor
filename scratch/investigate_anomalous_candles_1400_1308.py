"""
Investigação Aprofundada dos Dois Casos Anômalos:
1. Candle 14:00–14:02 BRT: Delta Vol = -9 com Delta Ticks = 0 (6.529 negócios em ambos).
2. Candle 13:08–13:10 BRT: Delta Vol = -1.900 com Delta Ticks = -274.
"""

import sys
sys.path.insert(0, ".")
import psycopg2
from datetime import datetime, timezone
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader

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

print("=== 1. INVESTIGAÇÃO DO CANDLE 14:00–14:02 BRT (DELTA VOL = -9, DELTA TICKS = 0) ===")
# 14:00 BRT eh 17:00 UTC
dt_1400_utc = datetime(2026, 10, 19, 17, 0, 0, tzinfo=timezone.utc)
dt_1402_utc = datetime(2026, 10, 19, 17, 2, 0, tzinfo=timezone.utc)

# Candle registrado no banco
cur.execute("SELECT open, high, low, close, volume_real, tick_volume FROM candles_m2 WHERE open_time_utc = %s;", (dt_1400_utc,))
c_1400 = cur.fetchone()
print(f"Candle registrado em candles_m2: O={c_1400[0]} H={c_1400[1]} L={c_1400[2]} C={c_1400[3]} | VolReal={c_1400[4]} | TickVol={c_1400[5]}")

# Ticks agregados no PostgreSQL
cur.execute("""
SELECT 
    (ARRAY_AGG(price ORDER BY time_msc ASC, occurrence_ordinal ASC))[1] as o,
    MAX(price) as h,
    MIN(price) as l,
    (ARRAY_AGG(price ORDER BY time_msc DESC, occurrence_ordinal DESC))[1] as c,
    COALESCE(SUM(volume_real), 0) as v_real,
    COUNT(*) as t_count
FROM market_ticks_raw
WHERE time_utc >= %s AND time_utc < %s;
""", (dt_1400_utc, dt_1402_utc))
t_1400 = cur.fetchone()
print(f"Ticks agregados no PostgreSQL:    O={t_1400[0]} H={t_1400[1]} L={t_1400[2]} C={t_1400[3]} | VolReal={t_1400[4]} | TickCount={t_1400[5]}")

# Rate atual no MT5
reader = MT5ReadOnlyReader(time_normalizer=normalizer)
if reader.initialize():
    msc_1400 = normalizer.utc_datetime_to_server_msc(dt_1400_utc)
    msc_1402 = normalizer.utc_datetime_to_server_msc(dt_1402_utc)
    rates = reader.fetch_rates_range("WINZ26", 2, msc_1400, msc_1402)
    if rates:
        r = rates[0]
        print(f"Rate atual retornado pelo MT5:    O={r['open']} H={r['high']} L={r['low']} C={r['close']} | VolReal={r['real_volume']} | TickVol={r['tick_volume']}")
    reader.shutdown()

# Verificar se ha negocios com volumes atipicos ou decimais
cur.execute("""
SELECT count(*), count(DISTINCT volume_real), min(volume_real), max(volume_real)
FROM market_ticks_raw
WHERE time_utc >= %s AND time_utc < %s;
""", (dt_1400_utc, dt_1402_utc))
print("Estatísticas de volume dos ticks 14:00-14:02:", cur.fetchone())

print("\n=== 2. INVESTIGAÇÃO DO CANDLE 13:08–13:10 BRT (DELTA VOL = -1.900, DELTA TICKS = -274) ===")
# 13:08 BRT eh 16:08 UTC
dt_1308_utc = datetime(2026, 10, 19, 16, 8, 0, tzinfo=timezone.utc)
dt_1310_utc = datetime(2026, 10, 19, 16, 10, 0, tzinfo=timezone.utc)

cur.execute("SELECT open, high, low, close, volume_real, tick_volume FROM candles_m2 WHERE open_time_utc = %s;", (dt_1308_utc,))
c_1308 = cur.fetchone()
print(f"Candle registrado em candles_m2: O={c_1308[0]} H={c_1308[1]} L={c_1308[2]} C={c_1308[3]} | VolReal={c_1308[4]} | TickVol={c_1308[5]}")

cur.execute("""
SELECT 
    (ARRAY_AGG(price ORDER BY time_msc ASC, occurrence_ordinal ASC))[1] as o,
    MAX(price) as h,
    MIN(price) as l,
    (ARRAY_AGG(price ORDER BY time_msc DESC, occurrence_ordinal DESC))[1] as c,
    COALESCE(SUM(volume_real), 0) as v_real,
    COUNT(*) as t_count
FROM market_ticks_raw
WHERE time_utc >= %s AND time_utc < %s;
""", (dt_1308_utc, dt_1310_utc))
t_1308 = cur.fetchone()
print(f"Ticks agregados no PostgreSQL:    O={t_1308[0]} H={t_1308[1]} L={t_1308[2]} C={t_1308[3]} | VolReal={t_1308[4]} | TickCount={t_1308[5]}")

# Rate atual no MT5 para 13:08
reader.initialize()
msc_1308 = normalizer.utc_datetime_to_server_msc(dt_1308_utc)
msc_1310 = normalizer.utc_datetime_to_server_msc(dt_1310_utc)
rates_1308 = reader.fetch_rates_range("WINZ26", 2, msc_1308, msc_1310)
reader.shutdown()
if rates_1308:
    r = rates_1308[0]
    print(f"Rate atual retornado pelo MT5:    O={r['open']} H={r['high']} L={r['low']} C={r['close']} | VolReal={r['real_volume']} | TickVol={r['tick_volume']}")

conn_pg.close()
