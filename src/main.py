import signal
import sys
import time
import logging
from datetime import datetime, timezone
from src.config import load_config
from src.engine import MarketCollectorEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("win-market-monitor")

class CollectorApp:
    def __init__(self):
        self.config = load_config()
        self.engine = MarketCollectorEngine(config=self.config)
        self.keep_running = True

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info(f"Sinal de parada ({signum}) recebido. Iniciando encerramento gracioso...")
        self.keep_running = False

    def run(self):
        logger.info(f"Inicializando win-market-monitor para o ativo {self.config.symbol}...")
        init_res = self.engine.initialize_and_recover()
        logger.info(f"Status da inicialização: {init_res}")

        poll_seconds = self.config.collector.poll_interval_ms / 1000.0

        try:
            while self.keep_running:
                # 1. Captura durável no SQLite WAL
                captured = self.engine.capture_step()
                
                # 2. Replicação para o PostgreSQL 17
                replicated = self.engine.replicate_pending_to_postgres()
                
                time.sleep(poll_seconds)
        except Exception as e:
            logger.error(f"Erro fatal no loop de coleta: {e}", exc_info=True)
        finally:
            logger.info("Executando dreno final de pendências antes da parada...")
            final_replicated = self.engine.replicate_pending_to_postgres()
            logger.info(f"Dreno final concluído: {final_replicated} registros replicados.")
            logger.info("win-market-monitor encerrado com segurança durável.")

if __name__ == '__main__':
    app = CollectorApp()
    app.run()
