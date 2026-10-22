import MetaTrader5 as mt5
from datetime import datetime, timezone
from collections import defaultdict
import zoneinfo

BRT = zoneinfo.ZoneInfo("America/Sao_Paulo")
mt5.initialize()

# Obter candles M5 dos ultimos dias
rates = mt5.copy_rates_from_pos("WINZ26", mt5.TIMEFRAME_M5, 0, 1000)
if rates is not None and len(rates) > 0:
    by_day = defaultdict(list)
    for r in rates:
        # Lembrete: time no MT5 da XP eh horario civil de Brasilia representado como epoch
        dt = datetime.fromtimestamp(r["time"], timezone.utc)
        day_str = dt.strftime("%Y-%m-%d")
        by_day[day_str].append(dt)
    for day in sorted(by_day.keys()):
        dts = by_day[day]
        print(f"{day}: primeiro={dts[0].strftime('%H:%M:%S')} | ultimo={dts[-1].strftime('%H:%M:%S')} | total_candles={len(dts)}")
else:
    print("Nenhum rate retornado")

mt5.shutdown()
