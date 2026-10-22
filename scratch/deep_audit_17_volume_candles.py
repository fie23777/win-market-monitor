"""
Auditoria Individual dos 17 Candles com Divergência de Volume:
Gera tabela exaustiva dos 17 candles (15 exclusivos + 2 mistos) com deltas exatos.
Investiga se há negócios específicos no milissegundo de corte ou se a hipótese permanece em aberto.
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

# Lista dos 17 candles (15 volume + 2 mistos)
volume_diff_open_times = [
    "10:26:00",
    "11:00:00",
    "11:44:00",
    "11:54:00",
    "12:22:00",
    "12:34:00",
    "13:06:00",
    "13:08:00",
    "13:10:00",
    "13:12:00",
    "13:28:00", # misto
    "13:46:00",
    "14:00:00",
    "14:22:00",
    "14:50:00",
    "15:10:00", # misto
    "15:14:00"
]

print(f"=== AUDITORIA INDIVIDUAL DOS 17 CANDLES COM DIVERGÊNCIA DE VOLUME ===")
print("Total a auditar:", len(volume_diff_open_times))

results = []

for o_str in volume_diff_open_times:
    dt_o_brt = datetime.strptime(f"2026-10-19 {o_str}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=BRT_TZ)
    dt_o_utc = dt_o_brt.astimezone(timezone.utc)
    
    # Obter candle cadastrado
    cur.execute("""
    SELECT close_time_utc, open, high, low, close, volume_real, tick_volume
    FROM candles_m2
    WHERE open_time_utc = %s;
    """, (dt_o_utc,))
    c_row = cur.fetchone()
    
    if not c_row:
        continue
        
    dt_c_utc = c_row[0]
    dt_c_brt = dt_c_utc.astimezone(BRT_TZ)
    c_vol = float(c_row[5])
    c_ticks = int(c_row[6])
    
    # Agregação estrita dos ticks [open_utc, close_utc)
    cur.execute("""
    SELECT COALESCE(SUM(volume_real), 0), COUNT(*)
    FROM market_ticks_raw
    WHERE time_utc >= %s AND time_utc < %s;
    """, (dt_o_utc, dt_c_utc))
    t_agg = cur.fetchone()
    t_vol = float(t_agg[0])
    t_ticks = int(t_agg[1])
    
    delta_vol = c_vol - t_vol
    delta_ticks = c_ticks - t_ticks
    
    # Checar negócios no primeiro segundo do intervalo [open_utc, open_utc + 1s)
    cur.execute("""
    SELECT price, volume_real, time_msc, occurrence_ordinal
    FROM market_ticks_raw
    WHERE time_utc >= %s AND time_utc < %s
    ORDER BY time_msc ASC, occurrence_ordinal ASC
    LIMIT 5;
    """, (dt_o_utc, dt_o_utc + timedelta(seconds=1)))
    first_ticks = cur.fetchall()
    
    # Checar negócios no último segundo [close_utc - 1s, close_utc)
    cur.execute("""
    SELECT price, volume_real, time_msc, occurrence_ordinal
    FROM market_ticks_raw
    WHERE time_utc >= %s AND time_utc < %s
    ORDER BY time_msc DESC, occurrence_ordinal DESC
    LIMIT 5;
    """, (dt_c_utc - timedelta(seconds=1), dt_c_utc))
    last_ticks = cur.fetchall()

    results.append({
        "interval_brt": f"{dt_o_brt.strftime('%H:%M:%S')} - {dt_c_brt.strftime('%H:%M:%S')}",
        "candle_vol": c_vol,
        "ticks_vol": t_vol,
        "delta_vol": delta_vol,
        "candle_ticks": c_ticks,
        "ticks_count": t_ticks,
        "delta_ticks": delta_ticks,
        "first_5_vols": [float(r[1]) for r in first_ticks],
        "last_5_vols": [float(r[1]) for r in last_ticks]
    })

print("\nTABELA INDIVIDUAL DOS 17 CANDLES COM DIVERGÊNCIA DE VOLUME:")
print(f"{'Intervalo BRT':<22} | {'Vol Candle':<12} | {'Vol Ticks':<12} | {'Delta Vol':<12} | {'Delta Ticks':<12}")
print("-" * 80)
for r in results:
    print(f"{r['interval_brt']:<22} | {r['candle_vol']:<12.1f} | {r['ticks_vol']:<12.1f} | {r['delta_vol']:<12.1f} | {r['delta_ticks']:<12}")

conn_pg.close()
