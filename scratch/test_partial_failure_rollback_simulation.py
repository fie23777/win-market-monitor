import sys
sys.path.insert(0, ".")
import psycopg2

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"

print("================================================================================")
print("  ENSAIO DE RESILIÊNCIA: SIMULAÇÃO DE FALHA INTERMEDIÁRIA E ROLLBACK NO CLONE")
print("================================================================================")

def execute_sql_file(conn, filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

conn = psycopg2.connect(SANDBOX_PG_URI)
cur = conn.cursor()

# 1. Executar rollback inicial no clone para garantir ponto de partida limpo
execute_sql_file(conn, "sql/migrations/h001/rollback_h001.sql")

cur.execute("""
    SELECT count(*), coalesce(sum(volume_real), 0)
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 12:00:00+00' AND time_utc <= '2026-10-19 21:35:00+00';
""")
init_raw = cur.fetchone()
print(f"Estado Inicial Limpo no Clone: {init_raw[0]} ticks, Volume {init_raw[1]:,.1f}")
assert init_raw[0] == 2093221
assert init_raw[1] == 19838713.0

# 2. Executar etapas 01, 02, 03 e 04 normalmente
print("\n--- APLICANDO ETAPAS 01, 02, 03 E 04 ---")
execute_sql_file(conn, "sql/migrations/h001/01_precondition_check.sql")
execute_sql_file(conn, "sql/migrations/h001/02_schema_evolution.sql")
execute_sql_file(conn, "sql/migrations/h001/03_reclassify_provenance_nc001.sql")
execute_sql_file(conn, "sql/migrations/h001/04_mark_duplicate_transport_artefacts_nc003.sql")

# Confirmar estado intermediário (NC-001 reclassificada, 166 duplicatas marcadas, mas ausentes ainda não injetados)
cur.execute("SELECT count(*) FROM duplicate_lineage;")
lineage_cnt = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM h001_reclassified_nc001_ticks;")
reclass_cnt = cur.fetchone()[0]
print(f"Estado Parcial Aplicado: {lineage_cnt} duplicatas mapeadas, {reclass_cnt} reclassificados NC-001")
assert lineage_cnt == 166
assert reclass_cnt == 44899

# 3. Simular falha crítica intermediária (abort na etapa 05)
print("\n--- SIMULANDO FALHA CRÍTICA INTERMEDIÁRIA NA ETAPA 05 ---")
try:
    cur.execute("RAISE EXCEPTION 'SIMULATED CRITICAL DISRUPTION: Conexão interrompida durante ingestão 05';")
except Exception as e:
    conn.rollback()
    print(f"  [ALERTA DE FALHA DETECTADA] {e}")

# 4. Acionar Rollback de Emergência
print("\n--- ACIONANDO ROLLBACK_H001.SQL PÓS-FALHA PARCIAL ---")
execute_sql_file(conn, "sql/migrations/h001/rollback_h001.sql")

# 5. Auditar restauração integral pós-falha parcial
cur.execute("""
    SELECT count(*), coalesce(sum(volume_real), 0)
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 12:00:00+00' AND time_utc <= '2026-10-19 21:35:00+00';
""")
post_fail_raw = cur.fetchone()
cur.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'DUPLICATE_TRANSPORT_ARTEFACT';")
rem_dups = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'RECOVERED_GAP_MT5';")
rem_nc1 = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'RECOVERED_HISTORICAL_MT5' AND time_utc >= '2026-10-19 12:00:00+00';")
rem_hist = cur.fetchone()[0]

print(f"\nEstado Auditado Pós-Rollback da Falha:")
print(f"  Ticks Brutos:     {post_fail_raw[0]} (Esperado original: 2.093.221)")
print(f"  Volume Bruto:     {post_fail_raw[1]:,.1f} (Esperado original: 19.838.713,0)")
print(f"  Ticks DUPLICATE_TRANSPORT_ARTEFACT: {rem_dups} (Esperado: 0)")
print(f"  Ticks RECOVERED_GAP_MT5: {rem_nc1} (Esperado: 0)")
print(f"  Ticks RECOVERED_HISTORICAL_MT5 preservados: {rem_hist} (Esperado: 759.845)")

assert post_fail_raw[0] == 2093221
assert post_fail_raw[1] == 19838713.0
assert rem_dups == 0
assert rem_nc1 == 0
assert rem_hist == 759845
print("\n  => SUCESSO: Rollback após falha intermediária comprovou restauração atômica sem estados zumbis!")

# 6. Reaplicar pacote completo de 01 a 08 para deixar o clone perfeitamente pronto
print("\n--- REAPLICANDO PACOTE COMPLETO DE MIGRAÇÃO (01 a 08) APÓS O ENSAIO ---")
scripts = [
    "sql/migrations/h001/01_precondition_check.sql",
    "sql/migrations/h001/02_schema_evolution.sql",
    "sql/migrations/h001/03_reclassify_provenance_nc001.sql",
    "sql/migrations/h001/04_mark_duplicate_transport_artefacts_nc003.sql",
    "sql/migrations/h001/05_insert_missing_historical_ticks_nc003.sql",
    "sql/migrations/h001/06_create_canonical_views.sql",
    "sql/migrations/h001/07_rebuild_candles_m2_v2.sql",
    "sql/migrations/h001/08_postcondition_check.sql",
]
for s in scripts:
    execute_sql_file(conn, s)

cur.execute("""
    SELECT count(*), coalesce(sum(volume_real), 0)
    FROM v_market_ticks_canonical
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 12:00:00+00' AND time_utc <= '2026-10-19 21:35:00+00';
""")
final_canon = cur.fetchone()
print(f"Estado Final Homologado no Clone: {final_canon[0]} ticks canônicos, {final_canon[1]:,.1f} volume")
assert final_canon[0] == 2093566
assert final_canon[1] == 19816479.0

conn.close()
print("================================================================================")
print("  ENSAIO DE FALHA INTERMEDIÁRIA E RECUPERAÇÃO CONCLUÍDO COM SUCESSO")
print("================================================================================")
