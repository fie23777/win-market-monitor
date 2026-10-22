import sys
sys.path.insert(0, ".")
import os
import time
from datetime import datetime, timedelta, timezone
import zoneinfo
from collections import Counter, defaultdict
import psycopg2

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.domain.models import compute_tick_event_signature

normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")
SESSION_START_BRT = datetime(2026, 10, 19, 9, 0, 0, tzinfo=BRT_TZ)
WINDOW_MINUTES = 5
TOTAL_WINDOWS = 115

PROD_PG_URI = "postgresql://postgres:password@127.0.0.1:5434/market_data"
conn_pg = psycopg2.connect(PROD_PG_URI)
cur_pg = conn_pg.cursor()

reader = MT5ReadOnlyReader(normalizer)
reader.initialize()
symbol = "WINZ26"
reader.select_symbol(symbol)

def is_mt5_trade_tick(flags: int, price: float, volume_real: float) -> bool:
    if price <= 0.0 or volume_real <= 0.0:
        return False
    return bool((flags & 32) or (flags & 64) or (flags & 8))

def determine_trade_side(flags: int) -> str:
    has_buy = bool(flags & 32)
    has_sell = bool(flags & 64)
    if has_buy and not has_sell:
        return "BUY"
    elif has_sell and not has_buy:
        return "SELL"
    else:
        return "CROSS"

def normalize_flags(flags: int) -> int:
    return flags & ~256

print("=" * 80)
print("AUDITORIA CANÔNICA DAS 115 JANELAS NO POSTGRESQL OPERACIONAL (PORTA 5434)")
print("=" * 80)

exact_matches = 0
empty_verified = 0
divergent = 0
total_pg_ticks = 0
total_pg_vol = 0.0

for w_idx in range(1, TOTAL_WINDOWS + 1):
    w_start_brt = SESSION_START_BRT + timedelta(minutes=(w_idx - 1) * WINDOW_MINUTES)
    w_end_brt = w_start_brt + timedelta(minutes=WINDOW_MINUTES)

    w_start_utc = w_start_brt.astimezone(timezone.utc)
    w_end_utc = w_end_brt.astimezone(timezone.utc)

    w_start_msc = normalizer.utc_datetime_to_server_msc(w_start_utc)
    w_end_msc = normalizer.utc_datetime_to_server_msc(w_end_utc)

    # 1. Consultar visão canônica na Produção
    cur_pg.execute("""
        SELECT time_msc, price, volume_real, trade_side, occurrence_ordinal
        FROM v_market_ticks_canonical
        WHERE symbol = %s AND time_utc >= %s AND time_utc < %s
        ORDER BY time_msc ASC, occurrence_ordinal ASC;
    """, (symbol, w_start_utc, w_end_utc))
    pg_rows = cur_pg.fetchall()

    # 2. Consultar histórico MT5
    raw_mt5 = reader.fetch_ticks_range(symbol, w_start_utc, w_end_utc)
    mt5_filtered = [t for t in raw_mt5 if w_start_msc <= t['time_msc'] < w_end_msc and is_mt5_trade_tick(t['flags'], t['last'], t['volume_real'])]

    mt5_groups = defaultdict(list)
    for t in mt5_filtered:
        time_msc = t['time_msc']
        price = float(t['last'])
        vol = float(t['volume_real'])
        flags = int(t['flags'])
        side = determine_trade_side(flags)
        sig = compute_tick_event_signature(time_msc, price, vol, normalize_flags(flags), side)
        mt5_groups[(time_msc, sig)].append((time_msc, price, vol, side))

    mt5_tick_items = []
    for (time_msc, sig), instances in mt5_groups.items():
        for ord_idx, inst in enumerate(instances, start=1):
            mt5_tick_items.append((inst[0], inst[1], inst[2], inst[3], ord_idx))

    pg_tick_items = [(r[0], r[1], float(r[2]), r[3], r[4]) for r in pg_rows]
    total_pg_ticks += len(pg_tick_items)
    total_pg_vol += sum(x[2] for x in pg_tick_items)

    pg_counter = Counter(pg_tick_items)
    mt5_counter = Counter(mt5_tick_items)

    missing_in_pg = sum((mt5_counter - pg_counter).values())
    extra_in_pg = sum((pg_counter - mt5_counter).values())

    if len(pg_tick_items) == 0 and len(mt5_tick_items) == 0:
        empty_verified += 1
    elif missing_in_pg == 0 and extra_in_pg == 0:
        exact_matches += 1
    else:
        divergent += 1
        print(f"Janela {w_idx} ({w_start_brt.strftime('%H:%M')} - {w_end_brt.strftime('%H:%M')}): DIVERGENTE! PG={len(pg_tick_items)} MT5={len(mt5_tick_items)} (faltam {missing_in_pg}, sobram {extra_in_pg})")

print(f"\nResultados da Auditoria das 115 Janelas na Produção:")
print(f"  EXACT_MATCH: {exact_matches}")
print(f"  EMPTY_VERIFIED: {empty_verified}")
print(f"  DIVERGENTES: {divergent}")
print(f"  Total Conformes: {exact_matches + empty_verified} / {TOTAL_WINDOWS}")

# Totais globais
cur_pg.execute("SELECT count(*), coalesce(sum(volume_real), 0) FROM v_market_ticks_canonical WHERE symbol = 'WINZ26';")
tot_canon, tot_vol = cur_pg.fetchone()
print(f"\nTotais v_market_ticks_canonical (5434):")
print(f"  Ticks: {tot_canon} (meta MT5: 2093566)")
print(f"  Volume: {tot_vol} (meta MT5: 19816479)")

cur_pg.execute("SELECT count(*), coalesce(sum(volume_real), 0) FROM candles_m2_v2 WHERE symbol = 'WINZ26';")
tot_c2, tot_c2_vol = cur_pg.fetchone()
print(f"\nTotais candles_m2_v2 (5434):")
print(f"  Candles: {tot_c2} (meta: 284)")
print(f"  Volume: {tot_c2_vol} (meta: 19816479)")

conn_pg.close()
reader.shutdown()

if divergent == 0 and tot_canon == 2093566 and tot_vol == 19816479 and tot_c2 == 284:
    print("\n" + "=" * 80)
    print("CONFORMIDADE 100% HOMOLOGADA NO POSTGRESQL OPERACIONAL!")
    print("=" * 80)
else:
    print("\nFALHA DE CONFORMIDADE!")
    sys.exit(1)
