import os
import sys
sys.path.insert(0, ".")
import shutil
import sqlite3
import psycopg2
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

normalizer = MT5TimeNormalizer()

print("=== PROTOCOLO DE FECHAMENTO DA SESSÃO OPERACIONAL ===")

# 1. Checagem do SQLite Spool
sqlite_path = Path("data/buffer/spool.db")
if not sqlite_path.exists():
    print(f"ERRO: Spool não encontrado em {sqlite_path}")
    sys.exit(1)

conn_sq = sqlite3.connect(sqlite_path)
cur_sq = conn_sq.cursor()

# Checkpoints
cur_sq.execute("SELECT stream_key, last_captured_msc, last_replicated_msc, updated_at_utc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
ckp = cur_sq.fetchone()

# Ticks stats
cur_sq.execute("SELECT COUNT(*), MIN(time_msc), MAX(time_msc) FROM spool_ticks;")
sq_ticks_total, sq_min_msc, sq_max_msc = cur_sq.fetchone()

# Pending backlog
cur_sq.execute("SELECT COUNT(*) FROM spool_ticks WHERE status = 'PENDING';")
sq_pending = cur_sq.fetchone()[0]

# Último negócio do spool
cur_sq.execute("SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, tick_fingerprint FROM spool_ticks ORDER BY time_msc DESC, occurrence_ordinal DESC LIMIT 1;")
last_tick = cur_sq.fetchone()

# Candles M2 no spool
cur_sq.execute("SELECT COUNT(*) FROM spool_candles_m2;")
sq_candles = cur_sq.fetchone()[0]

cur_sq.close()
conn_sq.close()

# 2. Checagem do PostgreSQL
conn_pg = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur_pg = conn_pg.cursor()

# Ticks no PG para WINZ26
cur_pg.execute("SELECT COUNT(*), MIN(time_msc), MAX(time_msc) FROM market_ticks_raw WHERE symbol = 'WINZ26';")
pg_ticks_total, pg_min_msc, pg_max_msc = cur_pg.fetchone()

# Candles no PG
cur_pg.execute("SELECT COUNT(*), MIN(open_time_utc), MAX(open_time_utc) FROM candles_m2 WHERE symbol = 'WINZ26';")
pg_candles_total, pg_min_candle, pg_max_candle = cur_pg.fetchone()

# Reconciliação sob o watermark
cur_pg.execute(f"SELECT COUNT(*) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc <= {ckp[2]};")
pg_ticks_under_wm = cur_pg.fetchone()[0]

cur_pg.close()
conn_pg.close()

dt_last_brt = normalizer.server_msc_to_utc_datetime(last_tick[0]).astimezone(normalizer.market_tz)
dt_last_utc = normalizer.server_msc_to_utc_datetime(last_tick[0])

print(f"\n1. ESTADO DOS CHECKPOINTS:")
print(f"   Stream Key: {ckp[0]}")
print(f"   last_captured_msc:   {ckp[1]}")
print(f"   last_replicated_msc: {ckp[2]}")
print(f"   Delta de Replicação: {ckp[1] - ckp[2]} ms")
print(f"   Backlog Pendente:    {sq_pending} itens (Status = PENDING)")

print(f"\n2. ÚLTIMO NEGÓCIO CAPTURADO:")
print(f"   time_msc:     {last_tick[0]}")
print(f"   Horário BRT:  {dt_last_brt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} BRT")
print(f"   Horário UTC:  {dt_last_utc.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC")
print(f"   Preço:        {last_tick[1]}")
print(f"   Volume Real:  {last_tick[2]}")
print(f"   Flags / Lado: {last_tick[3]} ({last_tick[4]})")
print(f"   Fingerprint:  {last_tick[6]}")

print(f"\n3. RECONCILIAÇÃO SQLITE vs POSTGRESQL:")
print(f"   Total de Ticks no SQLite Spool: {sq_ticks_total}")
print(f"   Total de Ticks no PostgreSQL:   {pg_ticks_total}")
print(f"   Ticks no PG sob W_durable ({ckp[2]}): {pg_ticks_under_wm}")
print(f"   Candles M2 no SQLite Spool:     {sq_candles}")
print(f"   Candles M2 no PostgreSQL:       {pg_candles_total}")
print(f"   Último Candle M2 PG:            {pg_max_candle}")

# 4. Backup para F:\win-market-monitor-backup
backup_dir = Path("F:/win-market-monitor-backup")
backup_dir.mkdir(parents=True, exist_ok=True)
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
session_backup_dir = backup_dir / f"session_closing_snapshot_{timestamp_str}"
session_backup_dir.mkdir(parents=True, exist_ok=True)

# Copiar spool.db e WAL
files_to_backup = [
    Path("data/buffer/spool.db"),
]
if Path("data/buffer/spool.db-wal").exists():
    files_to_backup.append(Path("data/buffer/spool.db-wal"))
if Path("data/buffer/spool.db-shm").exists():
    files_to_backup.append(Path("data/buffer/spool.db-shm"))

def compute_sha256(filepath):
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

print(f"\n4. CÓPIA DE BACKUP E VALIDAÇÃO SHA-256 EM {session_backup_dir}:")
hash_records = []
for src_f in files_to_backup:
    dest_f = session_backup_dir / src_f.name
    shutil.copy2(src_f, dest_f)
    src_hash = compute_sha256(src_f)
    dest_hash = compute_sha256(dest_f)
    match = (src_hash == dest_hash)
    hash_records.append((src_f.name, src_hash, dest_hash, match))
    print(f"   Arquivo: {src_f.name}")
    print(f"     Origem SHA-256:  {src_hash}")
    print(f"     Destino SHA-256: {dest_hash}")
    print(f"     Coincidência:    {'PERFEITA' if match else 'FALHA'}")

print("\n=== PROTOCOLO DE FECHAMENTO CONCLUÍDO COM SUCESSO ===")
