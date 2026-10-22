import sys
sys.path.insert(0, ".")
from src.config import load_config
from src.engine import MarketCollectorEngine

cfg = load_config()
eng = MarketCollectorEngine(cfg)
if eng.reader.initialize():
    eng.reader.select_symbol("WINZ26")
    m2_count = eng.sync_m2_candles()
    print(f"Candles M2 sincronizados com sucesso: {m2_count}")
    eng.shutdown()
else:
    print("Falha ao inicializar MT5")
