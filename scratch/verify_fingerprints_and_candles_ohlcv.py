"""
Verificação Forense Rigorosa:
1. Comparação 1-para-1 por tick_fingerprint (conteúdo exato) entre consulta independente ao MT5 e PostgreSQL (14:07:27 a 14:23:50 BRT).
2. Reconciliação OHLCV dos 11 candles M2 do intervalo (14:06 a 14:28 BRT) usando os limites temporais exatos de cada candle.
"""

import sys
sys.path.insert(0, ".")
import psycopg2
from datetime import datetime, timezone
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.reconciliation.tick_reconciler import TickReconciler

config = load_config()
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

# Janela exata do blackout
dt_start_brt = datetime(2026, 10, 19, 14, 7, 27, tzinfo=BRT_TZ)
dt_end_brt = datetime(2026, 10, 19, 14, 23, 50, tzinfo=BRT_TZ)
start_utc = dt_start_brt.astimezone(timezone.utc)
end_utc = dt_end_brt.astimezone(timezone.utc)
start_server_msc = normalizer.utc_datetime_to_server_msc(start_utc)
end_server_msc = normalizer.utc_datetime_to_server_msc(end_utc)

print("=== 1. AUDITORIA 1-PARA-1 POR FINGERPRINT CANÔNICO ===")

# 1. Obter fingerprints do PostgreSQL
conn_pg = psycopg2.connect(
    host=config.postgres.host,
    port=config.postgres.port,
    dbname=config.postgres.database,
    user=config.postgres.user,
    password=config.postgres.password
)
cur_pg = conn_pg.cursor()

cur_pg.execute("""
SELECT tick_fingerprint, price, volume_real, flags, trade_side, occurrence_ordinal
FROM market_ticks_raw
WHERE time_utc >= %s AND time_utc <= %s
ORDER BY time_msc ASC, occurrence_ordinal ASC, tick_fingerprint ASC;
""", (start_utc, end_utc))
pg_ticks = {r[0]: (float(r[1]), float(r[2]), int(r[3]), r[4], int(r[5])) for r in cur_pg.fetchall()}

# 2. Obter e reconciliar independentemente do MT5
reader = MT5ReadOnlyReader(time_normalizer=normalizer)
reader.initialize()
raw_mt5 = reader.fetch_ticks_range("WINZ26", start_server_msc, end_server_msc)
reader.shutdown()

reconciler = TickReconciler(time_normalizer=normalizer)
rec_mt5 = reconciler.reconcile_raw_ticks(raw_mt5, symbol="WINZ26", provenance="AUDIT")
mt5_ticks = {t.tick_fingerprint: (t.price, t.volume_real, t.flags, t.trade_side, t.occurrence_ordinal) for t in rec_mt5}

pg_set = set(pg_ticks.keys())
mt5_set = set(mt5_ticks.keys())

missing_in_pg = mt5_set - pg_set
extra_in_pg = pg_set - mt5_set
intersection = pg_set & mt5_set

print(f"Total fingerprints no MT5 independente: {len(mt5_set)}")
print(f"Total fingerprints no PostgreSQL:      {len(pg_set)}")
print(f"Coincidência exata (Interseção):       {len(intersection)}")
print(f"Ausentes no PostgreSQL:                {len(missing_in_pg)}")
print(f"Excedentes no PostgreSQL:              {len(extra_in_pg)}")

# Verificar divergência de atributos na interseção
attr_mismatches = 0
for fp in intersection:
    if pg_ticks[fp] != mt5_ticks[fp]:
        attr_mismatches += 1
print(f"Divergência de atributos (Preço/Volume/Flags/Lado/Ordinal): {attr_mismatches}")

print("\n=== 2. RECONCILIAÇÃO OHLCV DOS 11 CANDLES M2 (JANELA COMPLETA DE CADA CANDLE) ===")
# Obter os 11 candles M2 do banco
cur_pg.execute("""
SELECT open_time_utc, close_time_utc, open, high, low, close, volume_real, tick_volume
FROM candles_m2
WHERE open_time_utc >= '2026-10-19 17:06:00+00' AND open_time_utc <= '2026-10-19 17:26:00+00'
ORDER BY open_time_utc ASC;
""")
candles = cur_pg.fetchall()

ohlcv_mismatches = 0
for c in candles:
    c_open_utc = c[0]
    c_close_utc = c[1]
    c_open = float(c[2])
    c_high = float(c[3])
    c_low = float(c[4])
    c_close = float(c[5])
    c_vol_real = float(c[6])
    c_tick_vol = int(c[7])

    # Agregar ticks do PostgreSQL na janela completa [c_open_utc, c_close_utc)
    cur_pg.execute("""
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
    agg = cur_pg.fetchone()

    o_brt = c_open_utc.astimezone(BRT_TZ).strftime('%H:%M:%S')
    c_brt = c_close_utc.astimezone(BRT_TZ).strftime('%H:%M:%S')

    if agg and agg[0] is not None:
        t_o = float(agg[0])
        t_h = float(agg[1])
        t_l = float(agg[2])
        t_c = float(agg[3])
        t_vol_real = float(agg[4])
        t_count = int(agg[5])

        # Verificar se OHLC bate
        ohlc_match = (c_open == t_o and c_high == t_h and c_low == t_l and c_close == t_c)
        vol_match = (c_vol_real == t_vol_real)
        
        status = "MATCH" if ohlc_match and vol_match else "DIFF"
        if status == "DIFF":
            ohlcv_mismatches += 1
            print(f"Candle {o_brt}->{c_brt}: DIFF! Candle(O={c_open}, H={c_high}, L={c_low}, C={c_close}, V={c_vol_real}) vs Ticks(O={t_o}, H={t_h}, L={t_l}, C={t_c}, V={t_vol_real})")
        else:
            print(f"Candle {o_brt}->{c_brt}: PASS [OHLCV Exato] (Ticks={t_count}, Vol={t_vol_real})")
    else:
        print(f"Candle {o_brt}->{c_brt}: SEM TICKS NO PG!")
        ohlcv_mismatches += 1

print(f"\nTotal de divergências OHLCV nos candles: {ohlcv_mismatches}")
conn_pg.close()
