import os
import sys
import psycopg2
import sqlite3
import pandas as pd
from pathlib import Path

backup_dir = Path("F:/win-market-monitor-backup/session_closing_snapshot_20261019_214726")
csv_ticks = backup_dir / "market_ticks_raw_snapshot.csv"
csv_candles = backup_dir / "candles_m2_snapshot.csv"
spool_db = backup_dir / "spool.db"

print("=== VERIFICAÇÃO DE PRÉ-CONDIÇÕES E CONSISTÊNCIA DE SNAPSHOTS ===")

# 1. Contagem de linhas nos CSVs
with open(csv_candles, 'r', encoding='utf-8') as f:
    candles_lines = sum(1 for _ in f) - 1 # subtrai cabeçalho

print(f"Linhas em candles_m2_snapshot.csv: {candles_lines}")

# Contagem em streaming para o CSV grande de ticks
ticks_lines = 0
with open(csv_ticks, 'r', encoding='utf-8') as f:
    for _ in f:
        ticks_lines += 1
ticks_lines -= 1
print(f"Linhas em market_ticks_raw_snapshot.csv: {ticks_lines}")

# 2. Contagem no PostgreSQL ativo atual
conn_pg = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur_pg = conn_pg.cursor()

cur_pg.execute("SELECT COUNT(*) FROM market_ticks_raw;")
pg_ticks_now = cur_pg.fetchone()[0]

cur_pg.execute("SELECT COUNT(*) FROM candles_m2;")
pg_candles_now = cur_pg.fetchone()[0]

print(f"Registros atuais no PostgreSQL - ticks: {pg_ticks_now}, candles: {pg_candles_now}")

# Verificar se houve qualquer alteração ou inserção após o encerramento da sessão
cur_pg.execute("SELECT MAX(ingested_at_utc) FROM market_ticks_raw;")
pg_last_created_at = cur_pg.fetchone()[0]
print(f"Último ingested_at_utc em market_ticks_raw: {pg_last_created_at}")

cur_pg.execute("SELECT MAX(ingested_at_utc) FROM candles_m2;")
pg_last_created_candle = cur_pg.fetchone()[0]
print(f"Último ingested_at_utc em candles_m2: {pg_last_created_candle}")

# 3. Contagem no spool.db congelado
conn_sq = sqlite3.connect(f"file:{spool_db}?mode=ro", uri=True)
cur_sq = conn_sq.cursor()
cur_sq.execute("SELECT COUNT(*) FROM spool_ticks;")
sq_ticks = cur_sq.fetchone()[0]

cur_sq.execute("SELECT COUNT(*) FROM spool_candles_m2;")
sq_candles = cur_sq.fetchone()[0]

print(f"Registros no spool.db congelado - ticks: {sq_ticks}, candles: {sq_candles}")

cur_sq.close()
conn_sq.close()
cur_pg.close()
conn_pg.close()
