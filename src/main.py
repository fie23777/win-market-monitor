import signal
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from src.config import load_config
from src.engine import MarketCollectorEngine
from src.utils.process_lock import CollectorProcessLock, ProcessLockError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("win-market-monitor")

class CollectorApp:
    def __init__(self):
        self.config = load_config()
        self.keep_running = True
        
        lock_dir = Path(self.config.buffer.sqlite_path).parent
        self.lock = CollectorProcessLock(symbol=self.config.symbol, lock_dir=lock_dir)
        self.engine = MarketCollectorEngine(config=self.config)

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info(f"Sinal de parada ({signum}) recebido. Iniciando encerramento gracioso...")
        self.keep_running = False

    def run(self):
        # 1. Adquirir trava de exclusão mútua atômica
        try:
            self.lock.acquire()
        except ProcessLockError as ple:
            logger.error(f"Não foi possível iniciar o coletor: {ple}")
            sys.exit(2)
        except Exception as e:
            logger.critical(f"Erro inesperado ao adquirir trava atômica: {e}", exc_info=True)
            sys.exit(2)

        logger.info(f"Inicializando win-market-monitor para o ativo {self.config.symbol}...")
        try:
            init_res = self.engine.initialize_and_recover()
            logger.info(f"Status da inicialização: {init_res}")
        except Exception as e:
            logger.critical(f"Falha fatal ao inicializar o coletor: {e}", exc_info=True)
            self.engine.shutdown()
            self.lock.release()
            sys.exit(1)

        poll_seconds = self.config.collector.poll_interval_ms / 1000.0
        last_m2_sync = time.time()
        last_reconcile = time.time()

        try:
            while self.keep_running:
                # Verificar se há solicitação externa de shutdown gracioso
                if self.lock.is_shutdown_requested():
                    logger.info("Solicitação de encerramento gracioso detectada via controle externo.")
                    self.keep_running = False
                    break

                # Atualizar heartbeat da trava atômica
                self.lock.update_heartbeat()

                # 1. Captura durável no SQLite WAL
                captured = self.engine.capture_step()
                
                # 2. Replicação para o PostgreSQL 17
                replicated = self.engine.replicate_pending_to_postgres()

                # 3. Sincronização periódica de candles M2
                last_m2_sync, m2_count = self.engine.maybe_sync_m2_candles(last_m2_sync, interval_sec=60.0)

                # 4. Reconciliação histórica periódica (recuperação de eventos tardios e avanço de W_complete)
                last_reconcile, rec_res = self.engine.maybe_periodic_reconcile(last_reconcile, interval_sec=60.0)
                
                time.sleep(poll_seconds)
        except Exception as e:
            logger.error(f"Erro fatal no loop de coleta: {e}", exc_info=True)
        finally:
            self.stop_and_cleanup()

    def stop_and_cleanup(self) -> int:
        """
        Procedimento de parada segura e graciosa.
        Tenta drenar pendências para o PostgreSQL sem descartar dados caso esteja indisponível.
        Desliga componentes do engine e libera a trava de exclusão mútua.
        """
        logger.info("Executando procedimento de parada segura...")
        final_replicated = 0
        try:
            final_replicated = self.engine.replicate_pending_to_postgres()
            logger.info(f"Dreno final concluído: {final_replicated} registros replicados.")
        except Exception as e:
            logger.warning(
                f"PostgreSQL indisponível durante dreno final ({e}). "
                "Registros PENDING mantidos com integridade no buffer SQLite WAL."
            )
        finally:
            try:
                self.engine.shutdown()
            except Exception as e:
                logger.warning(f"Aviso ao desligar engine: {e}")
            self.lock.release()
            logger.info("win-market-monitor encerrado com segurança durável.")
        return final_replicated

if __name__ == '__main__':
    app = CollectorApp()
    app.run()

