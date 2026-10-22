"""
Auditoria Forense de Todos os Candles M2 da Sessão:
1. Reconciliação de 100% dos candles M2 do pregão de hoje contra reconstrução determinística via market_ticks_raw.
2. Análise detalhada dos candles 14:10–14:12 e 14:22–14:24.
3. Decomposição exata da divergência do candle 14:06 (22.729 contratos).
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

# 1. Varredura completa de todos os candles M2 de hoje
cur.execute("""
SELECT open_time_utc, close_time_utc, open, high, low, close, volume_real, tick_volume
FROM candles_m2
WHERE open_time_utc >= '2026-10-19 00:00:00+00'
ORDER BY open_time_utc ASC;
""")
all_candles = cur.fetchall()

print(f"=== VARREDURA COMPLETA DE CANDLES M2 DA SESSÃO ({len(all_candles)} candles cadastrados) ===")

divergences = []
matches = 0

for c in all_candles:
    c_open_utc = c[0]
    c_close_utc = c[1]
    c_o, c_h, c_l, c_c = float(c[2]), float(c[3]), float(c[4]), float(c[5])
    c_vol = float(c[6])
    c_ticks = int(c[7])

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
    """, (c_open_utc, c_close_utc))
    agg = cur.fetchone()

    o_brt = c_open_utc.astimezone(BRT_TZ).strftime('%H:%M:%S')
    c_brt = c_close_utc.astimezone(BRT_TZ).strftime('%H:%M:%S')

    if not agg or agg[0] is None:
        divergences.append({
            "candle": f"{o_brt}->{c_brt}",
            "type": "NO_TICKS_IN_DB",
            "details": "Nenhum tick encontrado no PostgreSQL para o intervalo"
        })
        continue

    t_o, t_h, t_l, t_c = float(agg[0]), float(agg[1]), float(agg[2]), float(agg[3])
    t_vol = float(agg[4])
    t_ticks = int(agg[5])

    ohlc_match = (c_o == t_o and c_h == t_h and c_l == t_l and c_c == t_c)
    vol_match = (c_vol == t_vol)

    if ohlc_match and vol_match:
        matches += 1
    else:
        diff_info = []
        if not ohlc_match:
            diff_info.append(f"OHLC: Candle({c_o}/{c_h}/{c_l}/{c_c}) vs Ticks({t_o}/{t_h}/{t_l}/{t_c})")
        if not vol_match:
            diff_info.append(f"Vol: Candle({c_vol}) vs Ticks({t_vol}) [delta: {c_vol - t_vol}]")
        divergences.append({
            "candle": f"{o_brt}->{c_brt}",
            "type": "MISMATCH",
            "details": " | ".join(diff_info)
        })

print(f"Candles em perfeita concordância OHLCV (MATCH): {matches} ({round(matches / len(all_candles) * 100, 2)}%)")
print(f"Candles com divergência OHLCV (DIFF):          {len(divergences)} ({round(len(divergences) / len(all_candles) * 100, 2)}%)")
print("\nDetalhamento dos candles divergentes em toda a sessão:")
for d in divergences:
    print(f"  [{d['candle']}] {d['type']}: {d['details']}")

# 2. Investigação específica dos candles 14:10 e 14:22 contra consulta atual ao MT5
print("\n=== INVESTIGAÇÃO ESPECÍFICA MT5 RATES PARA 14:10 E 14:22 BRT ===")
reader = MT5ReadOnlyReader(time_normalizer=normalizer)
if reader.initialize():
    dt_1410 = datetime(2026, 10, 19, 14, 10, 0, tzinfo=BRT_TZ)
    dt_1424 = datetime(2026, 10, 19, 14, 24, 0, tzinfo=BRT_TZ)
    msc_from = normalizer.utc_datetime_to_server_msc(dt_1410.astimezone(timezone.utc))
    msc_to = normalizer.utc_datetime_to_server_msc(dt_1424.astimezone(timezone.utc))
    rates = reader.fetch_rates_range("WINZ26", 2, msc_from, msc_to)
    reader.shutdown()

    if rates:
        for r in rates:
            r_dt = normalizer.server_msc_to_market_datetime(int(r["time"]) * 1000)
            print(f"  MT5 Rate Atual {r_dt.strftime('%H:%M:%S')}: O={r['open']} H={r['high']} L={r['low']} C={r['close']} | Vol={r['real_volume']} | Ticks={r['tick_volume']}")

conn_pg.close()
