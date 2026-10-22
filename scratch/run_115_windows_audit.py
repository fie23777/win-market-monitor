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

# Configurações de Fuso Horário e Conexão
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")
SESSION_DATE = datetime(2026, 10, 19).date()

SESSION_START_BRT = datetime(2026, 10, 19, 9, 0, 0, tzinfo=BRT_TZ)
SESSION_END_BRT = datetime(2026, 10, 19, 18, 35, 0, tzinfo=BRT_TZ)

WINDOW_MINUTES = 5
TOTAL_WINDOWS = 115

print("================================================================================")
print("  AUDITORIA HISTÓRICA PÓS-PREGÃO - WIN-MARKET-MONITOR")
print(f"  Sessão: {SESSION_DATE} | Horário: 09:00 às 18:35 BRT (115 Janelas de 5 min)")
print("  Modo: SOMENTE LEITURA (Zero modificações no banco ou snapshots)")
print("================================================================================")

# 1. Conexão PostgreSQL (somente leitura)
conn_pg = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur_pg = conn_pg.cursor()

# 2. Inicialização MT5 Reader (somente leitura)
reader = MT5ReadOnlyReader(normalizer)
if not reader.initialize():
    print("ERRO CRÍTICO: Falha ao inicializar MT5 Reader.")
    sys.exit(1)

symbol = "WINZ26"
if not reader.select_symbol(symbol):
    print(f"ERRO: Falha ao selecionar símbolo {symbol} no MT5.")
    sys.exit(1)

def is_mt5_trade_tick(flags: int, price: float, volume_real: float) -> bool:
    if price <= 0.0 or volume_real <= 0.0:
        return False
    # Flags de trade: TICK_FLAG_BUY(32), TICK_FLAG_SELL(64), TICK_FLAG_LAST(8)
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
    """Remove bit 256 (TICK_FLAG_VOLUME_REAL) adicionado apenas no storage offline do MT5."""
    return flags & ~256

def compute_multiset_hash(tick_items: list) -> str:
    """Gera hash SHA-256 do multiconjunto de trades ordenado."""
    h = hashlib.sha256()
    for item in tick_items:
        # item: (time_msc, price, volume_real, trade_side, occurrence_ordinal)
        payload = f"{item[0]}_{item[1]:.2f}_{item[2]:.4f}_{item[3]}_{item[4]}"
        h.update(payload.encode('utf-8'))
    return h.hexdigest()

audit_manifest = []
all_missing_ticks = []
all_extra_ticks = []
all_divergent_attributes = []

start_audit_wall = time.time()

for w_idx in range(1, TOTAL_WINDOWS + 1):
    w_start_brt = SESSION_START_BRT + timedelta(minutes=(w_idx - 1) * WINDOW_MINUTES)
    w_end_brt = w_start_brt + timedelta(minutes=WINDOW_MINUTES)

    w_start_utc = w_start_brt.astimezone(timezone.utc)
    w_end_utc = w_end_brt.astimezone(timezone.utc)

    w_start_msc = normalizer.utc_datetime_to_server_msc(w_start_utc)
    w_end_msc = normalizer.utc_datetime_to_server_msc(w_end_utc)

    t0_window = time.time()

    # --- CONSULTA POSTGRESQL ---
    cur_pg.execute("""
        SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, tick_fingerprint
        FROM market_ticks_raw
        WHERE symbol = %s AND time_utc >= %s AND time_utc < %s
        ORDER BY time_msc ASC, occurrence_ordinal ASC;
    """, (symbol, w_start_utc, w_end_utc))
    pg_rows = cur_pg.fetchall()

    pg_tick_items = []
    pg_vol_real = 0.0
    for r in pg_rows:
        time_msc, price, vol, flags, side, ord_idx, fp = r
        vol = float(vol)
        price = float(price)
        pg_vol_real += vol
        pg_tick_items.append((time_msc, price, vol, side, ord_idx, flags, fp))

    # --- CONSULTA MT5 ---
    # Usar datetime UTC normalizado para a query MT5
    raw_mt5 = reader.fetch_ticks_range(symbol, w_start_utc, w_end_utc)
    # Filtrar rigorosamente por time_msc na janela [w_start_msc, w_end_msc)
    mt5_filtered = [t for t in raw_mt5 if w_start_msc <= t['time_msc'] < w_end_msc and is_mt5_trade_tick(t['flags'], t['last'], t['volume_real'])]

    # Agrupar MT5 e atribuir occurrence_ordinal
    mt5_groups = defaultdict(list)
    for t in mt5_filtered:
        time_msc = t['time_msc']
        price = float(t['last'])
        vol = float(t['volume_real'])
        flags = int(t['flags'])
        side = determine_trade_side(flags)
        norm_flag = normalize_flags(flags)
        sig = compute_tick_event_signature(time_msc, price, vol, norm_flag, side)
        mt5_groups[(time_msc, sig)].append({
            "time_msc": time_msc,
            "price": price,
            "volume_real": vol,
            "flags": flags,
            "trade_side": side,
            "sig": sig
        })

    mt5_tick_items = []
    mt5_vol_real = 0.0
    for (time_msc, sig), instances in mt5_groups.items():
        for ord_idx, inst in enumerate(instances, start=1):
            fp = compute_tick_fingerprint(time_msc, sig, ord_idx)
            vol = inst['volume_real']
            mt5_vol_real += vol
            mt5_tick_items.append((time_msc, inst['price'], vol, inst['trade_side'], ord_idx, inst['flags'], fp))

    # Ordenar ambas as listas por time_msc, occurrence_ordinal
    pg_tick_items.sort(key=lambda x: (x[0], x[4]))
    mt5_tick_items.sort(key=lambda x: (x[0], x[4]))

    # Hashes de multiconjunto
    pg_hash = compute_multiset_hash([(x[0], x[1], x[2], x[3], x[4]) for x in pg_tick_items])
    mt5_hash = compute_multiset_hash([(x[0], x[1], x[2], x[3], x[4]) for x in mt5_tick_items])

    # Comparação de Multiconjuntos
    pg_counter = Counter((x[0], x[1], x[2], x[3], x[4]) for x in pg_tick_items)
    mt5_counter = Counter((x[0], x[1], x[2], x[3], x[4]) for x in mt5_tick_items)

    missing_in_pg = mt5_counter - pg_counter
    extra_in_pg = pg_counter - mt5_counter

    missing_count = sum(missing_in_pg.values())
    extra_count = sum(extra_in_pg.values())

    # Registrar detalhes individuais de ausentes
    if missing_count > 0:
        for item, count in missing_in_pg.items():
            dt_item_brt = normalizer.server_msc_to_utc_datetime(item[0]).astimezone(BRT_TZ)
            all_missing_ticks.append({
                "window": w_idx,
                "window_time_brt": f"{w_start_brt.strftime('%H:%M')} - {w_end_brt.strftime('%H:%M')}",
                "time_msc": item[0],
                "time_brt": dt_item_brt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                "price": item[1],
                "volume_real": item[2],
                "trade_side": item[3],
                "occurrence_ordinal": item[4],
                "count": count
            })

    if extra_count > 0:
        for item, count in extra_in_pg.items():
            dt_item_brt = normalizer.server_msc_to_utc_datetime(item[0]).astimezone(BRT_TZ)
            all_extra_ticks.append({
                "window": w_idx,
                "window_time_brt": f"{w_start_brt.strftime('%H:%M')} - {w_end_brt.strftime('%H:%M')}",
                "time_msc": item[0],
                "time_brt": dt_item_brt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                "price": item[1],
                "volume_real": item[2],
                "trade_side": item[3],
                "occurrence_ordinal": item[4],
                "count": count
            })

    query_latency = time.time() - t0_window

    # Classificação da Janela
    if len(pg_tick_items) == 0 and len(mt5_tick_items) == 0:
        status = "ZERO_NEGOCIOS"
    elif missing_count == 0 and extra_count == 0 and pg_hash == mt5_hash:
        status = "COINCIDENCIA_PERFEITA"
    else:
        status = "DIVERGENCIA_DETECTADA"

    manifest_entry = {
        "window_idx": w_idx,
        "time_start_brt": w_start_brt.strftime("%H:%M:%S"),
        "time_end_brt": w_end_brt.strftime("%H:%M:%S"),
        "ticks_mt5": len(mt5_tick_items),
        "ticks_pg": len(pg_tick_items),
        "delta_ticks": len(pg_tick_items) - len(mt5_tick_items),
        "vol_mt5": round(mt5_vol_real, 1),
        "vol_pg": round(pg_vol_real, 1),
        "delta_vol": round(pg_vol_real - mt5_vol_real, 1),
        "missing_in_pg": missing_count,
        "extra_in_pg": extra_count,
        "hash_mt5": mt5_hash[:16],
        "hash_pg": pg_hash[:16],
        "latency_sec": round(query_latency, 3),
        "status": status
    }
    audit_manifest.append(manifest_entry)

    # Log de progresso a cada 10 janelas ou se houver divergência
    if w_idx % 10 == 0 or status == "DIVERGENCIA_DETECTADA" or w_idx == TOTAL_WINDOWS:
        print(f"Janela #{w_idx:03d} ({manifest_entry['time_start_brt']}-{manifest_entry['time_end_brt']}): "
              f"MT5={manifest_entry['ticks_mt5']} | PG={manifest_entry['ticks_pg']} | "
              f"Delta={manifest_entry['delta_ticks']} | Status={status} ({query_latency:.2f}s)", flush=True)

reader.shutdown()
cur_pg.close()
conn_pg.close()

total_duration = time.time() - start_audit_wall

# Resumo da Auditoria
total_mt5_ticks = sum(m["ticks_mt5"] for m in audit_manifest)
total_pg_ticks = sum(m["ticks_pg"] for m in audit_manifest)
total_delta_ticks = total_pg_ticks - total_mt5_ticks
total_mt5_vol = sum(m["vol_mt5"] for m in audit_manifest)
total_pg_vol = sum(m["vol_pg"] for m in audit_manifest)

count_perfect = sum(1 for m in audit_manifest if m["status"] == "COINCIDENCIA_PERFEITA")
count_zero = sum(1 for m in audit_manifest if m["status"] == "ZERO_NEGOCIOS")
count_divergent = sum(1 for m in audit_manifest if m["status"] == "DIVERGENCIA_DETECTADA")

print("\n================================================================================")
print("  RESUMO EXECUTIVO DA AUDITORIA HISTÓRICA (115 JANELAS)")
print("================================================================================")
print(f"Duração Total da Auditoria: {total_duration:.2f} segundos")
print(f"Total de Janelas Auditadas: {len(audit_manifest)}")
print(f"  Janelas com Coincidência Perfeita: {count_perfect}")
print(f"  Janelas com Zero Negócios:        {count_zero}")
print(f"  Janelas com Divergência:           {count_divergent}")
print(f"Total de Ticks MT5 Auditados:       {total_mt5_ticks}")
print(f"Total de Ticks PostgreSQL Auditados: {total_pg_ticks}")
print(f"Delta Global de Ticks (PG - MT5):   {total_delta_ticks}")
print(f"Volume Real MT5:                    {total_mt5_vol:,.1f}")
print(f"Volume Real PostgreSQL:             {total_pg_vol:,.1f}")
print(f"Delta Global de Volume:             {total_pg_vol - total_mt5_vol:,.1f}")

# Salvar manifesto e detalhes em JSON para documentação auditável
out_dir = Path("scratch/audit_results_20261019")
out_dir.mkdir(parents=True, exist_ok=True)

with open(out_dir / "audit_manifest_115_windows.json", "w", encoding="utf-8") as f:
    json.dump(audit_manifest, f, indent=2, ensure_ascii=False)

with open(out_dir / "all_missing_ticks.json", "w", encoding="utf-8") as f:
    json.dump(all_missing_ticks, f, indent=2, ensure_ascii=False)

with open(out_dir / "all_extra_ticks.json", "w", encoding="utf-8") as f:
    json.dump(all_extra_ticks, f, indent=2, ensure_ascii=False)

print(f"\nManifesto e artefatos de auditoria gravados em: {out_dir}")
