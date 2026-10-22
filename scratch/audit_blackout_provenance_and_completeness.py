"""
Auditoria Forense de Proveniência e Completude pós-desconexão (14:07:27 a 14:23:50 BRT).
Investiga:
1. Contagem total e distribuição por proveniência no PostgreSQL.
2. Timestamps de mercado vs timestamps de ingestão local (para provar chegada tardia).
3. Consulta independente ao MT5 no mesmo intervalo exato para comparar completude 1-para-1.
4. Consistência dos candles M2 com os ticks do período.
"""

import sys
sys.path.insert(0, ".")
import psycopg2
import sqlite3
import json
from datetime import datetime, timezone
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.reconciliation.tick_reconciler import TickReconciler

config = load_config()
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

# Janela exata do blackout
dt_start_brt = datetime(2026, 10, 19, 14, 7, 27, tzinfo=BRT_TZ)
dt_end_brt = datetime(2026, 10, 19, 14, 23, 50, tzinfo=BRT_TZ)

start_utc = dt_start_brt.astimezone(timezone.utc)
end_utc = dt_end_brt.astimezone(timezone.utc)

start_server_msc = normalizer.utc_datetime_to_server_msc(start_utc)
end_server_msc = normalizer.utc_datetime_to_server_msc(end_utc)

print(f"=== JANELA DE AUDITORIA: {dt_start_brt.strftime('%H:%M:%S')} a {dt_end_brt.strftime('%H:%M:%S')} BRT ===")
print(f"Server MSC: {start_server_msc} a {end_server_msc}")

# 1. Auditoria no PostgreSQL
conn_pg = psycopg2.connect(
    host=config.postgres.host,
    port=config.postgres.port,
    dbname=config.postgres.database,
    user=config.postgres.user,
    password=config.postgres.password
)
cur_pg = conn_pg.cursor()

# Quantidade total e distribuição por proveniência
cur_pg.execute("""
SELECT 
    count(*) as total,
    count(*) FILTER (WHERE provenance = 'RECOVERED_HISTORICAL_MT5') as historical,
    count(*) FILTER (WHERE provenance = 'REALTIME_STREAM') as realtime,
    min(time_utc) as min_market_time,
    max(time_utc) as max_market_time,
    min(ingested_at_utc) as min_ingested,
    max(ingested_at_utc) as max_ingested,
    count(*) FILTER (WHERE ingested_at_utc >= '2026-10-19 17:23:50+00') as ingested_post_reconnect
FROM market_ticks_raw
WHERE time_utc >= %s AND time_utc <= %s;
""", (start_utc, end_utc))
pg_row = cur_pg.fetchone()

total_pg = pg_row[0]
hist_pg = pg_row[1]
rt_pg = pg_row[2]
min_mkt = pg_row[3]
max_mkt = pg_row[4]
min_ing = pg_row[5]
max_ing = pg_row[6]
post_rec_ing = pg_row[7]

print("\n--- 1. RESULTADOS NO POSTGRESQL ---")
print(f"Total de negócios registrados no intervalo: {total_pg}")
print(f"Classificados como RECOVERED_HISTORICAL_MT5: {hist_pg}")
print(f"Classificados como REALTIME_STREAM:          {rt_pg}")
if min_mkt:
    print(f"Primeiro timestamp de mercado: {min_mkt.astimezone(BRT_TZ).strftime('%H:%M:%S.%f')[:-3]}")
    print(f"Último timestamp de mercado:   {max_mkt.astimezone(BRT_TZ).strftime('%H:%M:%S.%f')[:-3]}")
if min_ing:
    print(f"Primeiro ingested_at_utc:      {min_ing.astimezone(BRT_TZ).strftime('%H:%M:%S.%f')[:-3]}")
    print(f"Último ingested_at_utc:        {max_ing.astimezone(BRT_TZ).strftime('%H:%M:%S.%f')[:-3]}")
print(f"Negócios com ingestão POSTERIOR à reconexão (>= 14:23:50 BRT): {post_rec_ing} ({round(post_rec_ing / total_pg * 100, 2) if total_pg else 0}%)")

# 2. Auditoria no SQLite Spool (created_at_utc)
conn_sq = sqlite3.connect(config.buffer.sqlite_path)
conn_sq.row_factory = sqlite3.Row
cur_sq = conn_sq.cursor()

cur_sq.execute("""
SELECT 
    count(*) as total,
    count(*) FILTER (WHERE provenance = 'RECOVERED_HISTORICAL_MT5') as historical,
    count(*) FILTER (WHERE provenance = 'REALTIME_STREAM') as realtime,
    min(created_at_utc) as min_created,
    max(created_at_utc) as max_created,
    count(*) FILTER (WHERE created_at_utc >= '2026-10-19T17:23:50') as created_post_reconnect
FROM spool_ticks
WHERE time_msc >= ? AND time_msc <= ?;
""", (start_server_msc, end_server_msc))
sq_row = cur_sq.fetchone()
conn_sq.close()

print("\n--- 2. RESULTADOS NO SQLITE SPOOL ---")
print(f"Total de negócios no Spool:               {sq_row['total']}")
print(f"Classificados como HISTORICAL:            {sq_row['historical']}")
print(f"Classificados como REALTIME:              {sq_row['realtime']}")
print(f"Primeiro created_at_utc:                  {sq_row['min_created']}")
print(f"Último created_at_utc:                    {sq_row['max_created']}")
print(f"Criados no spool APÓS reconexão:          {sq_row['created_post_reconnect']}")

# 3. Consulta Independente no Terminal MT5
print("\n--- 3. CONSULTA INDEPENDENTE DIRETA AO MT5 ---")
reader = MT5ReadOnlyReader(time_normalizer=normalizer)
if reader.initialize():
    raw_mt5 = reader.fetch_ticks_range("WINZ26", start_server_msc, end_server_msc)
    reader.shutdown()
    
    if raw_mt5:
        reconciler = TickReconciler(time_normalizer=normalizer)
        reconciled_mt5 = reconciler.reconcile_raw_ticks(
            raw_ticks=raw_mt5,
            symbol="WINZ26",
            provenance="AUDIT_VERIFY"
        )
        total_raw_mt5 = len(raw_mt5)
        total_rec_mt5 = len(reconciled_mt5)
        print(f"Ticks brutos retornados pelo MT5:       {total_raw_mt5}")
        print(f"Negócios filtrados/elegíveis no MT5:    {total_rec_mt5}")
        print(f"Negócios persistidos no PostgreSQL:     {total_pg}")
        diff = total_pg - total_rec_mt5
        print(f"Divergência (PG - MT5 independente):    {diff}")
    else:
        print("Aviso: MT5 não retornou ticks no intervalo (ou timeout).")
else:
    print("Aviso: Falha ao inicializar MT5 para consulta independente.")

# 4. Auditoria dos Candles M2 no Intervalo
print("\n--- 4. CANDLES M2 NO INTERVALO (14:06 às 14:26 BRT) ---")
cur_pg.execute("""
SELECT open_time_utc, close_time_utc, open, high, low, close, volume_real, tick_volume
FROM candles_m2
WHERE open_time_utc >= '2026-10-19 17:06:00+00' AND open_time_utc <= '2026-10-19 17:26:00+00'
ORDER BY open_time_utc ASC;
""")
candles = cur_pg.fetchall()
print(f"Total de candles M2 no intervalo: {len(candles)}")
for c in candles:
    o_brt = c[0].astimezone(BRT_TZ).strftime('%H:%M:%S')
    c_brt = c[1].astimezone(BRT_TZ).strftime('%H:%M:%S')
    print(f"  Candle {o_brt} -> {c_brt} | O: {c[2]} H: {c[3]} L: {c[4]} C: {c[5]} | VolReal: {c[6]} | Ticks: {c[7]}")

conn_pg.close()
