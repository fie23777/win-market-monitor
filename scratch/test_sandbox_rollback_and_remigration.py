import sys
sys.path.insert(0, ".")
import psycopg2

SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"

print("================================================================================")
print("  ENSAIO FORENSE COMPLETO DE MIGRAÇÃO, ROLLBACK E IDEMPOTÊNCIA NO CLONE")
print("================================================================================")

def execute_sql_file(conn, filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print(f"  [OK] Executado com sucesso: {filepath}")

conn = psycopg2.connect(SANDBOX_PG_URI)
cur = conn.cursor()

# 1. Verificar estado limpo inicial (snapshot recarregado)
cur.execute("""
    SELECT count(*), coalesce(sum(volume_real), 0)
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00';
""")
init_raw = cur.fetchone()
print(f"Estado Inicial no Sandbox Clone (Pregão 19/10/2026):")
print(f"  Ticks Brutos: {init_raw[0]} (Esperado original: 2.093.221)")
print(f"  Volume Bruto: {init_raw[1]:,.1f} (Esperado original: 19.838.713,0)")
assert init_raw[0] == 2093221, f"Erro inicial: {init_raw[0]} != 2093221"
assert init_raw[1] == 19838713.0, f"Erro inicial volume: {init_raw[1]} != 19838713.0"

# 2. Executar Migração Completa (01 a 08)
print("\n--- CICLO 1: APLICAÇÃO DO PACOTE DE MIGRAÇÃO (01 a 08) ---")
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
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00';
""")
c1_canon = cur.fetchone()
cur.execute("SELECT count(*), coalesce(sum(volume_real), 0) FROM candles_m2_v2;")
c1_candles = cur.fetchone()
cur.execute("SELECT count(*) FROM duplicate_lineage;")
c1_lineage = cur.fetchone()[0]

print(f"\nEstado após Ciclo 1 de Migração:")
print(f"  Ticks Canônicos:  {c1_canon[0]} (Meta MT5: 2.093.566)")
print(f"  Volume Canônico:  {c1_canon[1]:,.1f} (Meta MT5: 19.816.479,0)")
print(f"  Candles M2 v2:    {c1_candles[0]} (Meta: 284, Volume: {c1_candles[1]:,.1f})")
print(f"  Linhagem Dups:    {c1_lineage} (Meta: 166)")

assert c1_canon[0] == 2093566
assert c1_canon[1] == 19816479.0
assert c1_candles[0] == 284
assert c1_candles[1] == 19816479.0
assert c1_lineage == 166
print("  => CICLO 1 APROVADO COM SUCESSO!")

# 3. Executar Rollback Cirúrgico Total
print("\n--- CICLO 2: EXECUÇÃO DO ROLLBACK CIRÚRGICO TOTAL ---")
execute_sql_file(conn, "sql/migrations/h001/rollback_h001.sql")

cur.execute("""
    SELECT count(*), coalesce(sum(volume_real), 0)
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00';
""")
rb_raw = cur.fetchone()
cur.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'RECOVERED_HISTORICAL_MT5' AND time_utc >= '2026-10-19 12:00:00+00';")
rb_hist = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'DUPLICATE_TRANSPORT_ARTEFACT';")
rb_dups = cur.fetchone()[0]
cur.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'RECOVERED_GAP_MT5';")
rb_nc1 = cur.fetchone()[0]

print(f"\nEstado após Rollback Total:")
print(f"  Ticks Brutos:     {rb_raw[0]} (Esperado original: 2.093.221)")
print(f"  Volume Bruto:     {rb_raw[1]:,.1f} (Esperado original: 19.838.713,0)")
print(f"  Ticks com RECOVERED_HISTORICAL_MT5: {rb_hist} (Preservados legítimos originais: 759.845)")
print(f"  Ticks com DUPLICATE_TRANSPORT_ARTEFACT: {rb_dups} (Esperado: 0)")
print(f"  Ticks com RECOVERED_GAP_MT5: {rb_nc1} (Esperado: 0)")

assert rb_raw[0] == 2093221, f"Erro: {rb_raw[0]} != 2093221"
assert rb_raw[1] == 19838713.0, f"Erro: {rb_raw[1]} != 19838713.0"
assert rb_dups == 0
assert rb_nc1 == 0
assert rb_hist == 759845
print("  => ROLLBACK COMPROVADO: Base revertida 100% ao estado original sem nenhuma perda!")

# 4. Reexecutar Migração Completa (Ciclo 3)
print("\n--- CICLO 3: REMIGRAÇÃO COMPLETA PÓS-ROLLBACK ---")
for s in scripts:
    execute_sql_file(conn, s)

cur.execute("""
    SELECT count(*), coalesce(sum(volume_real), 0)
    FROM v_market_ticks_canonical
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00';
""")
c3_canon = cur.fetchone()
assert c3_canon[0] == 2093566
assert c3_canon[1] == 19816479.0
print("  => REMIGRAÇÃO PÓS-ROLLBACK BEM-SUCEDIDA!")

# 5. Teste de Idempotência: Reexecutar scripts 03 a 08 imediatamente
print("\n--- CICLO 4: TESTE DE IDEMPOTÊNCIA (SEGUNDA APLICAÇÃO IMEDIATA) ---")
for s in scripts[2:]:
    execute_sql_file(conn, s)

cur.execute("""
    SELECT count(*), coalesce(sum(volume_real), 0)
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00';
""")
c4_raw = cur.fetchone()
cur.execute("""
    SELECT count(*), coalesce(sum(volume_real), 0)
    FROM v_market_ticks_canonical
    WHERE symbol = 'WINZ26'
      AND time_utc >= '2026-10-19 12:00:00+00'
      AND time_utc <= '2026-10-19 21:35:00+00';
""")
c4_canon = cur.fetchone()
cur.execute("SELECT count(*) FROM candles_m2_v2;")
c4_candles = cur.fetchone()[0]

print(f"\nEstado após Segunda Execução Consecutiva:")
print(f"  Ticks Brutos Totais: {c4_raw[0]} (Esperado: 2.093.732)")
print(f"  Ticks Canônicos:     {c4_canon[0]} (Esperado: 2.093.566)")
print(f"  Volume Canônico:     {c4_canon[1]:,.1f} (Esperado: 19.816.479,0)")
print(f"  Candles M2 v2:       {c4_candles} (Esperado: 284)")

assert c4_raw[0] == 2093732
assert c4_canon[0] == 2093566
assert c4_canon[1] == 19816479.0
assert c4_candles == 284
print("  => IDEMPOTÊNCIA MATEMÁTICA E RELACIONAL COMPROVADA!")

conn.close()
print("\n================================================================================")
print("  ENSAIO DE MIGRAÇÃO, ROLLBACK E IDEMPOTÊNCIA 100% HOMOLOGADO NO CLONE")
print("================================================================================")
