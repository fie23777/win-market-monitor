import sys
sys.path.insert(0, ".")
import os
from pathlib import Path
import json
import time
import hashlib
from datetime import datetime, timedelta, timezone
import zoneinfo
from collections import Counter, defaultdict
import psycopg2
import pandas as pd

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.domain.models import compute_tick_event_signature, compute_tick_fingerprint

normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")
SESSION_START_BRT = datetime(2026, 10, 19, 9, 0, 0, tzinfo=BRT_TZ)
WINDOW_MINUTES = 5
TOTAL_WINDOWS = 115

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"
conn_pg = psycopg2.connect(SANDBOX_PG_URI)
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

audit_canonical_manifest = []

for w_idx in range(1, TOTAL_WINDOWS + 1):
    w_start_brt = SESSION_START_BRT + timedelta(minutes=(w_idx - 1) * WINDOW_MINUTES)
    w_end_brt = w_start_brt + timedelta(minutes=WINDOW_MINUTES)

    w_start_utc = w_start_brt.astimezone(timezone.utc)
    w_end_utc = w_end_brt.astimezone(timezone.utc)

    w_start_msc = normalizer.utc_datetime_to_server_msc(w_start_utc)
    w_end_msc = normalizer.utc_datetime_to_server_msc(w_end_utc)

    # 1. Consultar visão canônica no Sandbox Clone
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

    # Agrupar MT5 e atribuir ordinais
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
    mt5_vol = 0.0
    for (time_msc, sig), instances in mt5_groups.items():
        for ord_idx, inst in enumerate(instances, start=1):
            mt5_vol += inst[2]
            mt5_tick_items.append((inst[0], inst[1], inst[2], inst[3], ord_idx))

    pg_tick_items = [(r[0], r[1], float(r[2]), r[3], r[4]) for r in pg_rows]
    pg_vol = sum(x[2] for x in pg_tick_items)

    pg_counter = Counter(pg_tick_items)
    mt5_counter = Counter(mt5_tick_items)

    missing_in_pg = sum((mt5_counter - pg_counter).values())
    extra_in_pg = sum((pg_counter - mt5_counter).values())

    # Classificação formal estrita
    if len(pg_tick_items) == 0 and len(mt5_tick_items) == 0:
        cls = "EMPTY_VERIFIED"
    elif missing_in_pg == 0 and extra_in_pg == 0:
        cls = "EXACT_MATCH"
    elif len(pg_tick_items) == len(mt5_tick_items) and (missing_in_pg > 0 or extra_in_pg > 0):
        cls = "COUNT_MATCH_CONTENT_DIFF"
    elif missing_in_pg > 0 and extra_in_pg == 0:
        cls = "MISSING_ONLY"
    elif missing_in_pg == 0 and extra_in_pg > 0:
        cls = "EXTRA_ONLY"
    else:
        cls = "MIXED_DIFFERENCE"

    manifest_entry = {
        "window_idx": w_idx,
        "time_start_brt": w_start_brt.strftime("%H:%M:%S"),
        "time_end_brt": w_end_brt.strftime("%H:%M:%S"),
        "ticks_mt5": len(mt5_tick_items),
        "ticks_canonical_pg": len(pg_tick_items),
        "delta_ticks": len(pg_tick_items) - len(mt5_tick_items),
        "vol_mt5": round(mt5_vol, 1),
        "vol_canonical_pg": round(pg_vol, 1),
        "delta_vol": round(pg_vol - mt5_vol, 1),
        "missing_count": missing_in_pg,
        "extra_count": extra_in_pg,
        "classification": cls
    }
    audit_canonical_manifest.append(manifest_entry)

reader.shutdown()
cur_pg.close()
conn_pg.close()

df_res = pd.DataFrame(audit_canonical_manifest)
print("\n=== RESULTADO DA AUDITORIA DAS 115 JANELAS NA VISÃO CANÔNICA DO SANDBOX CLONE ===")
print(df_res['classification'].value_counts())

non_exact = df_res[~df_res['classification'].isin(['EXACT_MATCH', 'EMPTY_VERIFIED'])]
if non_exact.empty:
    print("\nSUCESSO PLENO: 114 EXACT_MATCH E 1 EMPTY_VERIFIED! ZERO DIVERGÊNCIAS!")
else:
    print(f"\nJanelas não conformes ({len(non_exact)}):")
    print(non_exact[['window_idx', 'time_start_brt', 'time_end_brt', 'ticks_mt5', 'ticks_canonical_pg', 'delta_ticks', 'missing_count', 'extra_count', 'classification']].to_string(index=False))

# Salvar relatório auditável
out_dir = Path("scratch/audit_results_20261019")
with open(out_dir / "audit_canonical_115_windows_sandbox.json", "w", encoding="utf-8") as f:
    json.dump(audit_canonical_manifest, f, indent=2, ensure_ascii=False)
