import sys
sys.path.insert(0, ".")
from datetime import datetime, timezone
import zoneinfo

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader

normalizer = MT5TimeNormalizer()
reader = MT5ReadOnlyReader(time_normalizer=normalizer)
reader.initialize()

# No dia 16/10/2026: das 18:20 as 18:35 BRT
# 18:20 BRT em 16/10/2026:
# server_msc = utc_datetime_to_server_msc
BRT = zoneinfo.ZoneInfo("America/Sao_Paulo")
dt_start = datetime(2026, 10, 16, 18, 20, 0, tzinfo=BRT)
dt_end = datetime(2026, 10, 16, 18, 35, 0, tzinfo=BRT)

start_msc = normalizer.utc_datetime_to_server_msc(dt_start.astimezone(timezone.utc))
end_msc = normalizer.utc_datetime_to_server_msc(dt_end.astimezone(timezone.utc))

print(f"Buscando de {start_msc} a {end_msc}...")
ticks = reader.fetch_ticks_range("WINZ26", start_msc, end_msc)
reader.shutdown()

if ticks is not None and len(ticks) > 0:
    print(f"Total ticks retornados: {len(ticks)}")
    t_first = normalizer.server_msc_to_market_datetime(ticks[0]["time_msc"])
    t_last = normalizer.server_msc_to_market_datetime(ticks[-1]["time_msc"])
    print(f"Primeiro tick: {t_first.strftime('%H:%M:%S.%f')[:-3]}")
    print(f"Ultimo tick:   {t_last.strftime('%H:%M:%S.%f')[:-3]}")
    print("Ultimos 5 ticks:")
    for t in ticks[-5:]:
        t_dt = normalizer.server_msc_to_market_datetime(t["time_msc"])
        print(f"  {t_dt.strftime('%H:%M:%S.%f')[:-3]} | Last={t['last']} | VolReal={t['volume_real']} | Flags={t['flags']}")
else:
    print("Nenhum tick retornado no intervalo")
