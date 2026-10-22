import sys
sys.path.insert(0, ".")
import json
from pathlib import Path
from collections import Counter
import pandas as pd

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

# Agrupar missing e extra por janela
missing_by_w = df_missing.groupby('window')['count'].sum().to_dict() if not df_missing.empty else {}
extra_by_w = df_extra.groupby('window')['count'].sum().to_dict() if not df_extra.empty else {}

classification_list = []

for m in manifest:
    w = m['window_idx']
    n_mt5 = m['ticks_mt5']
    n_pg = m['ticks_pg']
    m_count = missing_by_w.get(w, 0)
    e_count = extra_by_w.get(w, 0)

    # Classificação rigorosa
    if n_mt5 == 0 and n_pg == 0:
        cls = "EMPTY_VERIFIED"
    elif m_count == 0 and e_count == 0:
        cls = "EXACT_MATCH"
    elif n_mt5 == n_pg and (m_count > 0 or e_count > 0):
        cls = "COUNT_MATCH_CONTENT_DIFF"
    elif m_count > 0 and e_count == 0:
        cls = "MISSING_ONLY"
    elif m_count == 0 and e_count > 0:
        cls = "EXTRA_ONLY"
    elif m_count > 0 and e_count > 0 and n_mt5 != n_pg:
        cls = "MIXED_DIFFERENCE"
    else:
        cls = "INCONCLUSIVE"

    classification_list.append({
        "window_idx": w,
        "time_start_brt": m['time_start_brt'],
        "time_end_brt": m['time_end_brt'],
        "ticks_mt5": n_mt5,
        "ticks_pg": n_pg,
        "delta_ticks": n_pg - n_mt5,
        "missing_count": m_count,
        "extra_count": e_count,
        "classification": cls
    })

df_cls = pd.DataFrame(classification_list)

print("=== DISTRIBUIÇÃO ESTRITA DAS 115 JANELAS ===")
counts = df_cls['classification'].value_counts()
print(counts)
print(f"\nSoma total de janelas classificadas: {counts.sum()} / 115")

print("\n=== DETALHE DAS JANELAS QUE NÃO SÃO EXACT_MATCH / EMPTY_VERIFIED ===")
non_exact = df_cls[~df_cls['classification'].isin(['EXACT_MATCH', 'EMPTY_VERIFIED'])]
print(non_exact.to_string(index=False))

# Salvar classificação estruturada
with open(out_dir / "windows_strict_classification.json", "w", encoding="utf-8") as f:
    json.dump(classification_list, f, indent=2, ensure_ascii=False)
