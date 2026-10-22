import sys
sys.path.insert(0, ".")
from datetime import datetime, timezone
import zoneinfo

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader

normalizer = MT5TimeNormalizer()
reader = MT5ReadOnlyReader(time_normalizer=normalizer)
reader.initialize()

BRT = zoneinfo.ZoneInfo("America/Sao_Paulo")

for day in [9, 13, 14, 15, 16]:
    dt_start = datetime(2026, 10, day, 18, 20, 0, tzinfo=BRT)
    dt_end = datetime(2026, 10, day, 18, 45, 0, tzinfo=BRT)
    s_msc = normalizer.utc_datetime_to_server_msc(dt_start.astimezone(timezone.utc))
    e_msc = normalizer.utc_datetime_to_server_msc(dt_end.astimezone(timezone.utc))
    ticks = reader.fetch_ticks_range("WINZ26", s_msc, e_msc)
    if ticks:
        # Filtrar apenas deals (flags & 56 != 0)
        deals = [t for t in ticks if (t['flags'] & (mt5.TICK_FLAG_BUY | mt5.TICK_FLAG_SELL)) != 0 or t['last'] > 0]
        if deals:
            t_last = normalizer.server_msc_to_market_datetime(deals[-1]["time_msc"])
            print(f"Dia {day:02d}/10/2026: total_deals={len(deals)} | ultimo_negocio={t_last.strftime('%H:%M:%S.%f')[:-3]} | preco={deals[-1]['last']}")
        else:
            print(f"Dia {day:02d}/10/2026: sem deals apos 18:20")
    else:
        print(f"Dia {day:02d}/10/2026: nenhum tick retornado")

reader.shutdown()
