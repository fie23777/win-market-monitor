import sys
sys.path.insert(0, ".")
import psycopg2
import MetaTrader5 as mt5
from datetime import datetime, timezone
import zoneinfo
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

normalizer = MT5TimeNormalizer()
tz_brt = zoneinfo.ZoneInfo("America/Sao_Paulo")

print("=== 1. DIAGNÓSTICO DA JANELA 115 (LEILÃO 18:30-18:35 BRT) ===")
# Comparar o que está no PostgreSQL vs o que o MT5 retorna
conn_pg = psycopg2.connect("postgresql://postgres:password@127.0.0.1:5434/market_data")
cur_pg = conn_pg.cursor()

cur_pg.execute("""
    SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal, tick_fingerprint
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 21:30:00+00' AND time_utc < '2026-10-19 21:35:00+00'
    ORDER BY time_msc ASC, occurrence_ordinal ASC;
""")
pg_115 = cur_pg.fetchall()
print(f"Total no PG (Janela 115): {len(pg_115)} ticks, Volume Total: {sum(r[2] for r in pg_115)}")

# Consultar MT5
mt5.initialize()
dt_start = datetime(2026, 10, 19, 18, 30, 0, tzinfo=tz_brt)
dt_end = datetime(2026, 10, 19, 18, 35, 0, tzinfo=tz_brt)
raw_mt5_115 = mt5.copy_ticks_range("WINZ26", dt_start, dt_end, mt5.COPY_TICKS_ALL)
trades_mt5_115 = [t for t in raw_mt5_115 if ((t['flags'] & 32) or (t['flags'] & 64) or (t['flags'] & 8)) and t['last'] > 0 and t['volume_real'] > 0]
print(f"Total retornado pelo MT5 copy_ticks_range (Janela 115): {len(trades_mt5_115)} trades, Volume: {sum(t['volume_real'] for t in trades_mt5_115)}")

# Verificar também copy_ticks_from no MT5 para o instante do leilão (18:31:29)
t_leilao = datetime(2026, 10, 19, 18, 31, 28, tzinfo=tz_brt)
raw_from_leilao = mt5.copy_ticks_from("WINZ26", t_leilao, 100, mt5.COPY_TICKS_ALL)
trades_from_leilao = [t for t in raw_from_leilao if ((t['flags'] & 32) or (t['flags'] & 64) or (t['flags'] & 8)) and t['last'] > 0 and t['volume_real'] > 0]
print(f"Total retornado pelo MT5 copy_ticks_from (18:31:28): {len(trades_from_leilao)} trades, Volume: {sum(t['volume_real'] for t in trades_from_leilao)}")

print("\nComparação dos ticks do Leilão:")
print("PG (primeiros 5):", [(r[0], r[1], r[2], r[3], r[5]) for r in pg_115[:5]])
print("MT5 range (primeiros 5):", [(t['time_msc'], t['last'], t['volume_real'], t['flags']) for t in trades_mt5_115[:5]])
print("MT5 from (primeiros 5):", [(t['time_msc'], t['last'], t['volume_real'], t['flags']) for t in trades_from_leilao[:5]])

print("\n=== 2. DIAGNÓSTICO DE JANELA COM DELTA = 0 MAS STATUS DIVERGENCIA (ex: JANELA 2) ===")
# Janela 2: 09:05:00 a 09:10:00
w2_start = datetime(2026, 10, 19, 9, 5, 0, tzinfo=tz_brt)
w2_end = datetime(2026, 10, 19, 9, 10, 0, tzinfo=tz_brt)
cur_pg.execute("""
    SELECT time_msc, price, volume_real, flags, trade_side, occurrence_ordinal
    FROM market_ticks_raw
    WHERE symbol = 'WINZ26' AND time_utc >= %s AND time_utc < %s
    ORDER BY time_msc ASC, occurrence_ordinal ASC;
""", (w2_start.astimezone(timezone.utc), w2_end.astimezone(timezone.utc)))
pg_w2 = cur_pg.fetchall()
raw_mt5_w2 = mt5.copy_ticks_range("WINZ26", w2_start, w2_end, mt5.COPY_TICKS_ALL)
msc_start_w2 = normalizer.utc_datetime_to_server_msc(w2_start.astimezone(timezone.utc))
msc_end_w2 = normalizer.utc_datetime_to_server_msc(w2_end.astimezone(timezone.utc))
trades_w2 = [t for t in raw_mt5_w2 if msc_start_w2 <= t['time_msc'] < msc_end_w2 and ((t['flags'] & 32) or (t['flags'] & 64) or (t['flags'] & 8)) and t['last'] > 0 and t['volume_real'] > 0]

print(f"Janela 2: PG={len(pg_w2)}, MT5={len(trades_w2)}")

# Comparar Counter por (time_msc, price, volume_real, trade_side) IGNORANDO occurrence_ordinal
from collections import Counter
c_pg_no_ord = Counter((r[0], r[1], float(r[2]), r[4]) for r in pg_w2)
c_mt5_no_ord = Counter((t['time_msc'], float(t['last']), float(t['volume_real']),
    'BUY' if (t['flags'] & 32) and not (t['flags'] & 64) else ('SELL' if (t['flags'] & 64) and not (t['flags'] & 32) else 'CROSS'))
    for t in trades_w2)

diff_no_ord = (c_pg_no_ord - c_mt5_no_ord) + (c_mt5_no_ord - c_pg_no_ord)
print(f"Diferença na Janela 2 IGNORANDO occurrence_ordinal: {len(diff_no_ord)} divergências")

mt5.shutdown()
cur_pg.close()
conn_pg.close()
