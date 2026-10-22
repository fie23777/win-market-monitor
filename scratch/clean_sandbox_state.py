import psycopg2

conn = psycopg2.connect("postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS duplicate_lineage CASCADE;")
cur.execute("DROP TABLE IF EXISTS candles_m2_v2 CASCADE;")
cur.execute("DROP TABLE IF EXISTS audit_reconciliation_log CASCADE;")
cur.execute("DROP TABLE IF EXISTS h001_injected_ticks CASCADE;")
cur.execute("DROP TABLE IF EXISTS h001_reclassified_nc001_ticks CASCADE;")
cur.execute("DROP VIEW IF EXISTS v_market_ticks_canonical CASCADE;")
cur.execute("""
    UPDATE market_ticks_raw
    SET provenance = 'REALTIME_STREAM'
    WHERE provenance IN ('DUPLICATE_TRANSPORT_ARTEFACT', 'RECOVERED_GAP_MT5');
""")
cur.execute("""
    DELETE FROM market_ticks_raw
    WHERE provenance = 'RECOVERED_HISTORICAL_MT5' AND ingested_at_utc >= '2026-10-20 00:00:00+00';
""")
conn.commit()
conn.close()
print("Ambiente sandbox limpo e resetado com sucesso.")
