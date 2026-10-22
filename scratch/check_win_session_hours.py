import MetaTrader5 as mt5

if not mt5.initialize():
    print("MT5 initialize failed:", mt5.last_error())
    exit(1)

sym = mt5.symbol_info("WINZ26")
if sym is None:
    print("Symbol WINZ26 not found")
    mt5.shutdown()
    exit(1)

print("Symbol:", sym.name)
print("Description:", sym.description)


# Dias da semana: 0 = Segunda, 1 = Terca, ... 4 = Sexta
days = ["Segunda", "Terca", "Quarta", "Quinta", "Sexta", "Sabado", "Domingo"]
for day_idx in range(5):
    # Pode haver multiplas sessoes por dia (index 0, 1, 2)
    for sess_idx in range(3):
        q = mt5.symbol_info_session_quote("WINZ26", day_idx, sess_idx)
        t = mt5.symbol_info_session_trade("WINZ26", day_idx, sess_idx)
        if q or t:
            # q e t sao tuplas de datetime/time ou (from, to) em segundos desde 00:00
            def fmt_time(val):
                if val is None:
                    return "None"
                # MT5 session retorna datetime.time ou int (segundos do dia)
                if hasattr(val, "strftime"):
                    return val.strftime("%H:%M:%S")
                if isinstance(val, (int, float)):
                    h = int(val // 3600)
                    m = int((val % 3600) // 60)
                    s = int(val % 60)
                    return f"{h:02d}:{m:02d}:{s:02d}"
                return str(val)
            print(f"{days[day_idx]} (Sessao {sess_idx}): Quote=({fmt_time(q[0])} - {fmt_time(q[1])}) Trade=({fmt_time(t[0])} - {fmt_time(t[1])})")

mt5.shutdown()
