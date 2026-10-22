import json
import pandas as pd
from pathlib import Path

out_dir = Path("scratch/audit_results_20261019")
with open(out_dir / "audit_manifest_115_windows.json", "r", encoding="utf-8") as f:
    manifest = json.load(f)

with open(out_dir / "all_missing_ticks.json", "r", encoding="utf-8") as f:
    missing = json.load(f)

with open(out_dir / "all_extra_ticks.json", "r", encoding="utf-8") as f:
    extra = json.load(f)

df_manifest = pd.DataFrame(manifest)

print("=== 1. ANÁLISE GERAL DO MANIFESTO DAS 115 JANELAS ===")
print("Contagem por Status:")
print(df_manifest['status'].value_counts())

# Janelas onde a contagem de ticks foi IDÊNTICA (Delta Ticks == 0)
same_count = df_manifest[df_manifest['delta_ticks'] == 0]
print(f"\nJanelas com contagem de ticks 100% IDÊNTICA (Delta = 0): {len(same_count)} / 115")

# Janelas onde Delta Ticks != 0
diff_count = df_manifest[df_manifest['delta_ticks'] != 0]
print(f"Janelas com diferença na contagem de ticks (Delta != 0): {len(diff_count)} / 115")

print("\nTabela das janelas com Delta Ticks != 0:")
print(diff_count[['window_idx', 'time_start_brt', 'time_end_brt', 'ticks_mt5', 'ticks_pg', 'delta_ticks', 'vol_mt5', 'vol_pg', 'delta_vol']].to_string(index=False))

print(f"\n=== 2. TOTAL DE REGISTROS AUSENTES NO POSTGRESQL (MISSING): {len(missing)} ===")
df_missing = pd.DataFrame(missing)
if not df_missing.empty:
    print(df_missing.groupby('window_time_brt').size())
    print("\nPrimeiros 5 registros ausentes no PG:")
    print(df_missing[['window', 'window_time_brt', 'time_brt', 'price', 'volume_real', 'trade_side', 'occurrence_ordinal']].head(10).to_string(index=False))

print(f"\n=== 3. TOTAL DE REGISTROS EXCEDENTES NO POSTGRESQL (EXTRA): {len(extra)} ===")
df_extra = pd.DataFrame(extra)
if not df_extra.empty:
    print(df_extra.groupby('window_time_brt').size())
    print("\nRegistros excedentes no PG (ex: leilão 18:30-18:35):")
    print(df_extra[['window', 'window_time_brt', 'time_brt', 'price', 'volume_real', 'trade_side', 'occurrence_ordinal']].head(10).to_string(index=False))
