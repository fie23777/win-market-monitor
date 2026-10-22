import MetaTrader5 as mt5
from datetime import datetime, timezone
import zoneinfo

BRT = zoneinfo.ZoneInfo("America/Sao_Paulo")
mt5.initialize()

# Obter candles M2 dos dias 15/10 e 16/10 no final do dia
rates = mt5.copy_rates_from_pos("WINZ26", 2, 0, 1500) # 2 = TIMEFRAME_M2
if rates is not None and len(rates) > 0:
    from collections import defaultdict
    by_day = defaultdict(list)
    for r in rates:
        dt = datetime.fromtimestamp(r["time"], timezone.utc)
        day_str = dt.strftime("%Y-%m-%d")
        by_day[day_str].append((dt, r))
    
    for day in sorted(by_day.keys()):
        candles = by_day[day]
        last_3 = candles[-3:]
        print(f"=== {day} (Total M2: {len(candles)}) ===")
        for dt, r in last_3:
            print(f"  Candle: {dt.strftime('%H:%M:%S')} | Open: {r['open']} | Close: {r['close']} | VolReal: {r['real_volume']} | Ticks: {r['tick_volume']}")
else:
    print("Nenhum rate retornado")

mt5.shutdown()
