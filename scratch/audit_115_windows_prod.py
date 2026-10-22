import json
import psycopg2

def main():
    print("=" * 80)
    print("AUDITORIA DAS 115 JANELAS NO POSTGRESQL OPERACIONAL (PORTA 5434)")
    print("=" * 80)

    # 1. Carregar referência congelada do MT5
    with open("c:/Users/fie23/Projetos/win-market-monitor/data/audit/audit_reference_dataset_20261019.json", "r", encoding="utf-8") as f:
        ref_data = json.load(f)

    ref_windows = {w["window_id"]: w for w in ref_data["windows"]}

    # 2. Conectar na porta 5434
    conn = psycopg2.connect(
        host="127.0.0.1", port=5434, dbname="market_data", user="postgres", password="password"
    )

    divergent_count = 0
    total_canon_ticks = 0
    total_canon_vol = 0.0

    with conn.cursor() as cur:
        # Validar cada janela
        for wid, wref in ref_windows.items():
            start_utc = wref["start_time_utc"]
            end_utc = wref["end_time_utc"]
            cur.execute("""
                SELECT count(*), coalesce(sum(volume_real), 0)
                FROM v_market_ticks_canonical
                WHERE symbol = 'WINZ26'
                  AND time_utc >= %s
                  AND time_utc <= %s;
            """, (start_utc, end_utc))
            pg_cnt, pg_vol = cur.fetchone()

            if pg_cnt != wref["mt5_tick_count"] or abs(pg_vol - wref["mt5_volume_real"]) > 1e-4:
                divergent_count += 1
                print(f"DIVERGÊNCIA na janela {wid}: MT5={wref['mt5_tick_count']}/{wref['mt5_volume_real']} vs PG={pg_cnt}/{pg_vol}")

        # Totais globais
        cur.execute("SELECT count(*), coalesce(sum(volume_real), 0) FROM v_market_ticks_canonical WHERE symbol = 'WINZ26';")
        total_canon_ticks, total_canon_vol = cur.fetchone()

        cur.execute("SELECT count(*), coalesce(sum(volume_real), 0) FROM candles_m2_v2 WHERE symbol = 'WINZ26';")
        total_candles, total_candles_vol = cur.fetchone()

        cur.execute("SELECT count(*), coalesce(sum(duplicate_volume_real), 0) FROM duplicate_lineage;")
        dup_cnt, dup_vol = cur.fetchone()

        cur.execute("SELECT count(*) FROM h001_injected_ticks;")
        inj_cnt = cur.fetchone()[0]

        cur.execute("SELECT count(*) FROM h001_reclassified_nc001_ticks;")
        reclass_cnt = cur.fetchone()[0]

    conn.close()

    print(f"Janelas Auditadas: {len(ref_windows)}")
    print(f"Janelas Conformes: {len(ref_windows) - divergent_count} / {len(ref_windows)}")
    print(f"Janelas Divergentes: {divergent_count}")
    print(f"Total Ticks Canônicos: {total_canon_ticks} (esperado: 2093566)")
    print(f"Total Volume Canônico: {total_canon_vol} (esperado: 19816479)")
    print(f"Total Candles M2 v2: {total_candles} (esperado: 284)")
    print(f"Total Volume Candles M2 v2: {total_candles_vol} (esperado: 19816479)")
    print(f"Linhagem de Duplicatas: {dup_cnt} registros | Volume: {dup_vol}")
    print(f"Negócios Ausentes Injetados: {inj_cnt} registros")
    print(f"Reclassificados NC-001: {reclass_cnt} registros")

    if divergent_count == 0 and total_canon_ticks == 2093566 and total_canon_vol == 19816479 and total_candles == 284:
        print("\n" + "=" * 80)
        print("AUDITORIA PÓS-MIGRAÇÃO 100% HOMOLOGADA NO BANCO OPERACIONAL!")
        print("=" * 80)
    else:
        print("\nFALHA NA AUDITORIA!")

if __name__ == "__main__":
    main()
