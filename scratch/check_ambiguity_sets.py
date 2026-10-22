import json
import pandas as pd

with open('scratch/refined_284_candles_audit.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

df = pd.DataFrame(data)

only_open = df[df['open_ambiguous'] & ~df['close_ambiguous']]
only_close = df[~df['open_ambiguous'] & df['close_ambiguous']]
both = df[df['open_ambiguous'] & df['close_ambiguous']]
neither = df[~df['open_ambiguous'] & ~df['close_ambiguous']]

print("=== DECOMPOSIÇÃO MATEMÁTICA DOS CONJUNTOS DE AMBIGUIDADE ===")
print(f"Total Candles Analisados:                         {len(df)}")
print(f"1. Apenas Ambiguidade de OPEN (Open Ambiguous & NOT Close):  {len(only_open)}")
print(f"2. Apenas Ambiguidade de CLOSE (Close Ambiguous & NOT Open): {len(only_close)}")
print(f"3. Interseção (AMBAS: Open Ambiguous & Close Ambiguous):     {len(both)}")
print(f"4. Nenhuma Ambiguidade (OHLC 100% Determinístico):           {len(neither)}")
print("-------------------------------------------------------------")
total_efetivo = len(only_open) + len(only_close) + len(both)
print(f"Total com Ambiguidade Efetiva (Open OU Close):               {total_efetivo}")
print(f"Verificação de Exaustividade: {total_efetivo} + {len(neither)} = {total_efetivo + len(neither)}")

if len(both) > 0:
    print("\nCandles na Interseção (Ambas as ambiguidades):")
    for _, r in both.iterrows():
        print(f"  [{r['dt_brt']}] O={r['open']} C={r['close']} Vol={r['volume_real']}")
else:
    print("\nOs conjuntos são ESTRITAMENTE DISJUNTOS (Interseção = 0).")
