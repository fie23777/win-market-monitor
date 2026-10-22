"""
Diagnóstico Causal Candidata NC-002: Candle 14:06–14:08 BRT
Investiga:
1. Volume dos ticks antes da desconexão (14:06:00 a 14:07:27).
2. Volume dos ticks durante a desconexão (14:07:27 a 14:08:00) entregues pós-reconexão.
3. Timestamp em que o candle 14:06 foi inserido no PostgreSQL (ingested_at_utc).
4. O que o rate nativo do MT5 retorna atualmente para esse candle.
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

# 1. Dados do Candle registrado em candles_m2
cur.execute("""
SELECT open_time_utc, close_time_utc, open, high, low, close, volume_real, tick_volume, received_at_utc, ingested_at_utc
FROM candles_m2
WHERE open_time_utc = '2026-10-19 17:06:00+00';
""")
c_row = cur.fetchone()

print("=== 1. REGISTRO DO CANDLE 14:06 EM candles_m2 ===")
if c_row:
    print(f"Open: {c_row[2]} | High: {c_row[3]} | Low: {c_row[4]} | Close: {c_row[5]}")
    print(f"Volume Real gravado no candle: {c_row[6]}")
    print(f"Tick Volume gravado no candle: {c_row[7]}")
    print(f"received_at_utc: {c_row[8]}")
    print(f"ingested_at_utc: {c_row[9].astimezone(BRT_TZ).strftime('%H:%M:%S.%f')[:-3] if c_row[9] else 'None'}")

# 2. Decomposição dos ticks no PostgreSQL
# Sub-intervalo A: 14:06:00 a 14:07:27 (antes da queda)
cur.execute("""
SELECT count(*), coalesce(sum(volume_real), 0), min(time_utc), max(time_utc)
FROM market_ticks_raw
WHERE time_utc >= '2026-10-19 17:06:00+00' AND time_utc < '2026-10-19 17:07:27+00';
""")
sub_a = cur.fetchone()

# Sub-intervalo B: 14:07:27 a 14:08:00 (durante a queda, entregues pós-reconexão)
cur.execute("""
SELECT count(*), coalesce(sum(volume_real), 0), min(time_utc), max(time_utc), min(ingested_at_utc), max(ingested_at_utc)
FROM market_ticks_raw
WHERE time_utc >= '2026-10-19 17:07:27+00' AND time_utc < '2026-10-19 17:08:00+00';
""")
sub_b = cur.fetchone()

print("\n=== 2. DECOMPOSIÇÃO DOS TICKS NO POSTGRESQL ===")
print(f"Sub-intervalo A (14:06:00 às 14:07:27 - Antes da queda):")
print(f"  Ticks: {sub_a[0]} | Volume Real: {sub_a[1]}")
print(f"Sub-intervalo B (14:07:27 às 14:08:00 - Durante o blackout):")
print(f"  Ticks: {sub_b[0]} | Volume Real: {sub_b[1]}")
if sub_b[4]:
    print(f"  Ingestão física dos ticks de B: de {sub_b[4].astimezone(BRT_TZ).strftime('%H:%M:%S')} até {sub_b[5].astimezone(BRT_TZ).strftime('%H:%M:%S')}")

print(f"\nSoma dos sub-intervalos A + B: {float(sub_a[1]) + float(sub_b[1])} contratos")
print(f"Volume gravado no candle:       {c_row[6]} contratos")

# 3. O que o MT5 rates retorna AGORA para o candle das 14:06?
print("\n=== 3. CONSULTA ATUAL AO MT5 RATES PARA 14:06 BRT ===")
reader = MT5ReadOnlyReader(time_normalizer=normalizer)
if reader.initialize():
    # 14:06 BRT em server msc:
    dt_1406 = datetime(2026, 10, 19, 14, 6, 0, tzinfo=BRT_TZ)
    dt_1410 = datetime(2026, 10, 19, 14, 10, 0, tzinfo=BRT_TZ)
    msc_from = normalizer.utc_datetime_to_server_msc(dt_1406.astimezone(timezone.utc))
    msc_to = normalizer.utc_datetime_to_server_msc(dt_1410.astimezone(timezone.utc))
    rates = reader.fetch_rates_range("WINZ26", 2, msc_from, msc_to)
    reader.shutdown()
    
    if rates:
        for r in rates:
            r_dt = normalizer.server_msc_to_market_datetime(int(r["time"]) * 1000)
            print(f"  Rate MT5 {r_dt.strftime('%H:%M:%S')}: O={r['open']} H={r['high']} L={r['low']} C={r['close']} | VolReal={r['real_volume']} | Ticks={r['tick_volume']}")
    else:
        print("Nenhum rate retornado pelo MT5")

conn_pg.close()
