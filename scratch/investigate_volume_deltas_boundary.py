"""
Investigação Causal das 17 Divergências de Volume:
Testa a hipótese de transferência de volume na fronteira limítrofe de candle.
Verifica se somando o candle [i] e [i+1] o volume coincide, e se os ticks no segundo exato
da virada de minuto explicam os deltas de volume.
"""

import sys
sys.path.insert(0, ".")
import psycopg2
from datetime import datetime, timezone, timedelta
import zoneinfo

from src.config import load_config
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

config = load_config()
normalizer = MT5TimeNormalizer()
BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

conn_pg = psycopg2.connect(
    host=config.postgres.host,
    port=config.postgres.port,
    dbname=config.postgres.database,
    user=config.postgres.user,
    password=config.postgres.password
)
cur = conn_pg.cursor()

# Lista dos candles com divergência de volume observados
volume_candles_to_test = [
    ("10:26:00", "10:28:00"),
    ("11:00:00", "11:02:00"),
    ("11:44:00", "11:46:00"),
    ("11:54:00", "11:56:00"),
    ("12:22:00", "12:24:00"),
    ("12:34:00", "12:36:00"),
    ("13:06:00", "13:08:00"),
    ("13:08:00", "13:10:00"),
    ("13:10:00", "13:12:00"),
    ("13:12:00", "13:14:00"),
    ("13:46:00", "13:48:00"),
    ("14:22:00", "14:24:00"),
    ("14:50:00", "14:52:00"),
    ("15:14:00", "15:16:00")
]

print("=== INVESTIGAÇÃO DE TRANSFERÊNCIA DE FRONTEIRA TEMPORAL ===")

for o_str, c_str in volume_candles_to_test:
    # Converter para UTC
    dt_o = datetime.strptime(f"2026-10-19 {o_str}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=BRT_TZ).astimezone(timezone.utc)
    dt_c = datetime.strptime(f"2026-10-19 {c_str}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=BRT_TZ).astimezone(timezone.utc)
    
    # Candle gravado no banco
    cur.execute("SELECT volume_real, tick_volume FROM candles_m2 WHERE open_time_utc = %s;", (dt_o,))
    c_row = cur.fetchone()
    c_vol = float(c_row[0]) if c_row else 0.0

    # Ticks com janela estrita [dt_o, dt_c)
    cur.execute("SELECT COALESCE(SUM(volume_real), 0) FROM market_ticks_raw WHERE time_utc >= %s AND time_utc < %s;", (dt_o, dt_c))
    t_vol_strict = float(cur.fetchone()[0])
    delta_strict = c_vol - t_vol_strict

    # Ticks no segundo exato anterior (dt_o - 1s a dt_o)
    cur.execute("SELECT COALESCE(SUM(volume_real), 0) FROM market_ticks_raw WHERE time_utc >= %s AND time_utc < %s;", (dt_o - timedelta(seconds=1), dt_o))
    vol_prev_sec = float(cur.fetchone()[0])

    # Ticks no segundo exato posterior (dt_c a dt_c + 1s)
    cur.execute("SELECT COALESCE(SUM(volume_real), 0) FROM market_ticks_raw WHERE time_utc >= %s AND time_utc < %s;", (dt_c, dt_c + timedelta(seconds=1)))
    vol_next_sec = float(cur.fetchone()[0])

    # Teste de janela com inclusão à direita: [dt_o, dt_c] (fechada nos dois lados)
    cur.execute("SELECT COALESCE(SUM(volume_real), 0) FROM market_ticks_raw WHERE time_utc >= %s AND time_utc <= %s;", (dt_o, dt_c))
    t_vol_inclusive = float(cur.fetchone()[0])

    print(f"\nCandle {o_str}->{c_str}:")
    print(f"  Candle Vol: {c_vol} | Ticks Vol (estrito [T0, T1)): {t_vol_strict} | Delta: {delta_strict}")
    print(f"  Volume no 1s anterior (T0-1s a T0): {vol_prev_sec}")
    print(f"  Volume no 1s posterior (T1 a T1+1s): {vol_next_sec}")
    print(f"  Ticks Vol inclusivo [T0, T1]:       {t_vol_inclusive}")

# Teste em bloco para o cluster 13:06 a 13:14 (4 candles adjacentes com deltas)
# 13:06 (+105), 13:08 (-1900), 13:10 (+88), 13:12 (+12)
dt_1306 = datetime(2026, 10, 19, 13, 6, 0, tzinfo=BRT_TZ).astimezone(timezone.utc)
dt_1314 = datetime(2026, 10, 19, 13, 14, 0, tzinfo=BRT_TZ).astimezone(timezone.utc)

cur.execute("SELECT SUM(volume_real) FROM candles_m2 WHERE open_time_utc >= %s AND open_time_utc < %s;", (dt_1306, dt_1314))
sum_candles_1306_1314 = float(cur.fetchone()[0])

cur.execute("SELECT SUM(volume_real) FROM market_ticks_raw WHERE time_utc >= %s AND time_utc < %s;", (dt_1306, dt_1314))
sum_ticks_1306_1314 = float(cur.fetchone()[0])

print("\n=== CLUSTER ADJACENTE 13:06 ÀS 13:14 BRT (8 MINUTOS) ===")
print(f"Soma dos 4 Candles no Banco: {sum_candles_1306_1314}")
print(f"Soma dos Ticks no PostgreSQL: {sum_ticks_1306_1314}")
print(f"Delta Agregado no Período:    {sum_candles_1306_1314 - sum_ticks_1306_1314}")

conn_pg.close()
