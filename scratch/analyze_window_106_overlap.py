import sys
sys.path.insert(0, ".")
import json
import psycopg2
import pandas as pd

with open("scratch/audit_results_20261019/all_extra_ticks.json", "r", encoding="utf-8") as f:
    all_extra = json.load(f)

with open("scratch/audit_results_20261019/all_missing_ticks.json", "r", encoding="utf-8") as f:
    all_missing = json.load(f)

extra_106 = [x for x in all_extra if x['window'] == 106]
missing_106 = [x for x in all_missing if x['window'] == 106]

df_extra = pd.DataFrame(extra_106)
df_missing = pd.DataFrame(missing_106)

print(f"Janela 106 - Extra no PG: {len(df_extra)}, Missing no PG: {len(df_missing)}")

# Verificar interseção de time_msc entre os extra e missing da Janela 106
msc_extra = set(df_extra['time_msc'])
msc_missing = set(df_missing['time_msc'])

common_msc = msc_extra & msc_missing
print(f"time_msc em comum entre extra e missing da Janela 106: {len(common_msc)}")

# Verificar quantos dos extra coincidem com missing em (time_msc, price, volume_real, trade_side)
df_extra_common = df_extra[df_extra['time_msc'].isin(common_msc)].sort_values(['time_msc', 'price', 'volume_real'])
df_missing_common = df_missing[df_missing['time_msc'].isin(common_msc)].sort_values(['time_msc', 'price', 'volume_real'])

print(f"Registros extra com time_msc compartilhado: {len(df_extra_common)}")
print(f"Registros missing com time_msc compartilhado: {len(df_missing_common)}")

# Comparar preços e volumes
matches = 0
for idx, r_ex in df_extra_common.iterrows():
    m = df_missing_common[
        (df_missing_common['time_msc'] == r_ex['time_msc']) &
        (df_missing_common['price'] == r_ex['price']) &
        (df_missing_common['volume_real'] == r_ex['volume_real']) &
        (df_missing_common['trade_side'] == r_ex['trade_side'])
    ]
    if not m.empty:
        matches += 1

print(f"Trades com coincidência econômica idêntica (time_msc, price, volume_real, trade_side) entre extra e missing: {matches}")
print(f"Volume dos trades coincidentes: {df_extra_common['volume_real'].sum()}")

# Verificar o que sobrou
print(f"\nExtra sem par em missing: {len(df_extra) - matches}")
print(f"Missing sem par em extra: {len(df_missing) - matches}")
