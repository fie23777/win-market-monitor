import json
import pandas as pd
from pathlib import Path
from collections import Counter

out_dir = Path("scratch/audit_results_20261019")
with open(out_dir / "audit_manifest_115_windows.json", "r", encoding="utf-8") as f:
    manifest = json.load(f)

with open(out_dir / "all_missing_ticks.json", "r", encoding="utf-8") as f:
    missing = json.load(f)

with open(out_dir / "all_extra_ticks.json", "r", encoding="utf-8") as f:
    extra = json.load(f)

df_manifest = pd.DataFrame(manifest)
df_missing = pd.DataFrame(missing)
df_extra = pd.DataFrame(extra)

print("=== 1. VERIFICAÇÃO FORMAL DE MULTICONJUNTOS (CONTAGEM) ===")
n_mt5 = df_manifest['ticks_mt5'].sum()
n_pg = df_manifest['ticks_pg'].sum()
delta_n = n_mt5 - n_pg

count_missing = df_missing['count'].sum() if not df_missing.empty else 0
count_extra = df_extra['count'].sum() if not df_extra.empty else 0

print(f"N_MT5:     {n_mt5}")
print(f"N_PG:      {n_pg}")
print(f"N_MT5 - N_PG (Delta Líquido): {delta_n}")
print(f"N_missing: {count_missing} (linhas em all_missing_ticks: {len(df_missing)})")
print(f"N_extra:   {count_extra} (linhas em all_extra_ticks: {len(df_extra)})")
print(f"N_missing - N_extra: {count_missing - count_extra}")
print(f"Identidade N_MT5 - N_PG == N_missing - N_extra: {delta_n == (count_missing - count_extra)}")

print("\n=== 2. VERIFICAÇÃO FORMAL DE VOLUMES ===")
v_mt5 = df_manifest['vol_mt5'].sum()
v_pg = df_manifest['vol_pg'].sum()
delta_v = v_pg - v_mt5

# Volume em missing e extra
# Cada registro em missing/extra tem 'volume_real' e 'count'
vol_missing = (df_missing['volume_real'] * df_missing['count']).sum() if not df_missing.empty else 0.0
vol_extra = (df_extra['volume_real'] * df_extra['count']).sum() if not df_extra.empty else 0.0

print(f"V_MT5: {v_mt5:,.1f}")
print(f"V_PG:  {v_pg:,.1f}")
print(f"V_PG - V_MT5 (Delta Volume Líquido): {delta_v:,.1f}")
print(f"V_missing: {vol_missing:,.1f}")
print(f"V_extra:   {vol_extra:,.1f}")
print(f"V_extra - V_missing: {vol_extra - vol_missing:,.1f}")
print(f"Diferença residual de volume: {delta_v - (vol_extra - vol_missing):,.1f}")

print("\n=== 3. DETALHAMENTO DE EXTRA_TICKS POR JANELA ===")
if not df_extra.empty:
    print(df_extra.groupby(['window', 'window_time_brt'])[['count', 'volume_real']].sum())

print("\n=== 4. DETALHAMENTO DE MISSING_TICKS POR JANELA ===")
if not df_missing.empty:
    print(df_missing.groupby(['window', 'window_time_brt'])[['count', 'volume_real']].sum())
