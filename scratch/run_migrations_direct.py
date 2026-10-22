import subprocess
import time

scripts = [
    "01_precondition_check.sql",
    "02_schema_evolution.sql",
    "03_reclassify_provenance_nc001.sql",
    "04_mark_duplicate_transport_artefacts_nc003.sql",
    "05_insert_missing_historical_ticks_nc003.sql",
    "06_create_canonical_views.sql",
    "07_rebuild_candles_m2_v2.sql",
    "08_postcondition_check.sql"
]

print("=" * 80)
print("INICIANDO EXECUÇÃO DIRETA DOS SCRIPTS DE MIGRAÇÃO NO POSTGRESQL 5434")
print("=" * 80)

for s in scripts:
    print(f"\n>>> Executando {s}...")
    t0 = time.time()
    cmd = [
        "docker", "exec", "win_market_postgres",
        "psql", "-U", "postgres", "-d", "market_data", "-v", "ON_ERROR_STOP=1", "-f", f"/tmp/h001/{s}"
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    t1 = time.time()
    print(f"Tempo: {t1 - t0:.2f}s | Código: {res.returncode}")
    if res.stdout:
        print(f"STDOUT:\n{res.stdout.strip()}")
    if res.stderr:
        print(f"STDERR:\n{res.stderr.strip()}")
    if res.returncode != 0:
        print(f"FALHA NO SCRIPT {s}! Interrompendo execução.")
        break
else:
    print("\n" + "=" * 80)
    print("TODOS OS SCRIPTS EXECUTADOS COM SUCESSO!")
    print("=" * 80)
