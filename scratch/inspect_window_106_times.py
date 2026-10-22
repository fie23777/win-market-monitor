import json
import pandas as pd

with open("scratch/audit_results_20261019/all_extra_ticks.json", "r", encoding="utf-8") as f:
    all_extra = json.load(f)

with open("scratch/audit_results_20261019/all_missing_ticks.json", "r", encoding="utf-8") as f:
    all_missing = json.load(f)

extra_106 = pd.DataFrame([x for x in all_extra if x['window'] == 106])
missing_106 = pd.DataFrame([x for x in all_missing if x['window'] == 106])

print("--- 138 EXTRA TICKS NA JANELA 106 ---")
print(f"Início: {extra_106['time_brt'].min()} | Fim: {extra_106['time_brt'].max()}")
print(f"Volume Real Total: {extra_106['volume_real'].sum()}")
print("Distribuição por segundo:")
print(extra_106['time_brt'].str.slice(0, 19).value_counts())

print("\n--- 132 MISSING TICKS NA JANELA 106 ---")
print(f"Início: {missing_106['time_brt'].min()} | Fim: {missing_106['time_brt'].max()}")
print(f"Volume Real Total: {missing_106['volume_real'].sum()}")
print("Distribuição por segundo:")
print(missing_106['time_brt'].str.slice(0, 19).value_counts())
