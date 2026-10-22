"""
Sessão Operacional Supervisionada Estendida - win-market-monitor (WINZ26)
Modo: Somente Leitura (Passivo) | Retomada a partir de checkpoints existentes
"""

import sys
sys.path.insert(0, ".")
import os
import time
import signal
import psutil
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

from src.config import load_config
from src.engine import MarketCollectorEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("supervised_session")

class SupervisedSessionApp:
    def __init__(self):
        self.config = load_config()
        self.engine = MarketCollectorEngine(config=self.config)
        self.keep_running = True
        self.process = psutil.Process()
        self.telemetry_interval_sec = 900.0 # 15 minutos
        self.m2_sync_interval_sec = 60.0    # 1 minuto
        self.telemetry_history = []
        self.telemetry_file = Path("C:/Users/fie23/.gemini/antigravity-ide/brain/ef92ffd5-2a0e-47f2-97b3-b07361021f8d/supervised_session_telemetry.json")

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info(f"Sinal de interrupção recebido ({signum}). Disparando encerramento gracioso...")
        self.keep_running = False

    def emit_telemetry_snapshot(self, cycle_count: int, latencies: list, last_tick_seen_msc: int):
        try:
            ckp = self.engine.spool.get_checkpoints(self.engine.symbol)
            pending_ticks = len(self.engine.spool.get_pending_ticks(limit=100))
            current_mem = self.process.memory_info().rss / (1024 * 1024)
            avg_lat = sum(latencies[-500:]) / len(latencies[-500:]) if latencies else 0.0
            max_lat = max(latencies[-500:]) if latencies else 0.0
            
            # Atraso em relação ao relógio civil
            now_server_msc = self.engine.time_normalizer.utc_datetime_to_server_msc(datetime.now(timezone.utc))
            market_lag_ms = max(0, now_server_msc - (ckp.last_captured_msc if ckp.last_captured_msc > 0 else now_server_msc))

            now_utc = datetime.now(timezone.utc)
            now_brt = now_utc.astimezone(self.engine.time_normalizer.market_tz)

            snapshot = {
                "timestamp_brt": now_brt.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp_utc": now_utc.isoformat(),
                "pid": os.getpid(),
                "mt5_connected": self.engine.reader.is_connected(),
                "last_captured_msc": ckp.last_captured_msc,
                "last_replicated_msc": ckp.last_replicated_msc,
                "replication_lag_ms": ckp.last_captured_msc - ckp.last_replicated_msc,
                "market_lag_ms": market_lag_ms,
                "spool_pending_sample": pending_ticks,
                "rss_memory_mb": round(current_mem, 2),
                "avg_polling_latency_ms": round(avg_lat, 2),
                "max_polling_latency_ms": round(max_lat, 2),
                "total_cycles": cycle_count
            }

            self.telemetry_history.append(snapshot)
            with open(self.telemetry_file, "w", encoding="utf-8") as f:
                json.dump(self.telemetry_history, f, indent=2, ensure_ascii=False)

            logger.info(
                f"[TELEMETRIA 15m] PID: {snapshot['pid']} | MT5: {snapshot['mt5_connected']} | "
                f"Captured: {ckp.last_captured_msc} | Replicated: {ckp.last_replicated_msc} | "
                f"MarketLag: {market_lag_ms}ms | Pending: {pending_ticks} | RAM: {current_mem:.1f}MB | Latency: {avg_lat:.2f}ms"
            )
        except Exception as e:
            logger.warning(f"Erro ao emitir telemetria: {e}")

    def run(self):
        pid = os.getpid()
        boot_wall_utc = datetime.now(timezone.utc)
        boot_wall_brt = boot_wall_utc.astimezone(self.engine.time_normalizer.market_tz)
        logger.info(f"=== INICIANDO SESSÃO OPERACIONAL SUPERVISIONADA (PID: {pid}) ===")
        logger.info(f"Horário de Início: {boot_wall_brt.strftime('%Y-%m-%d %H:%M:%S %Z')} ({boot_wall_utc.isoformat()})")
        logger.info("Modo de Operação: PASSIVO / SOMENTE LEITURA. Zero funções de ordens.")

        # 1. Boot e Recuperação do Intervalo de Desligamento
        try:
            init_res = self.engine.initialize_and_recover()
            logger.info(f"Inicialização e recuperação concluídas: {init_res}")
        except Exception as e:
            logger.critical(f"Falha fatal ao inicializar sessão supervisionada: {e}", exc_info=True)
            self.engine.shutdown()
            sys.exit(1)

        poll_sec = self.config.collector.poll_interval_ms / 1000.0
        last_telemetry_time = time.time()
        last_m2_sync_time = time.time()
        latencies = []
        cycle_count = 0
        last_tick_seen_msc = self.engine.spool.get_checkpoints(self.engine.symbol).last_captured_msc

        logger.info(f"Entrando em loop de streaming supervisionado contínuo (polling a cada {self.config.collector.poll_interval_ms}ms)...")

        # Emitir primeiro snapshot de telemetria inicial
        self.emit_telemetry_snapshot(cycle_count, latencies, last_tick_seen_msc)

        try:
            while self.keep_running:
                c_start = time.time()

                # 1. Captura durável no SQLite WAL
                captured = self.engine.capture_step()

                # 2. Replicação remota para o PostgreSQL 17
                replicated = self.engine.replicate_pending_to_postgres(batch_size=self.config.buffer.batch_size)

                c_lat = (time.time() - c_start) * 1000.0
                latencies.append(c_lat)
                cycle_count += 1

                # 3. Sincronização periódica de candles M2
                last_m2_sync_time, m2_synced = self.engine.maybe_sync_m2_candles(last_m2_sync_time, interval_sec=self.m2_sync_interval_sec)
                if m2_synced > 0:
                    logger.info(f"Candles M2 sincronizados: +{m2_synced} novos candles fechados.")

                # 4. Telemetria periódica (a cada 15 minutos)
                if time.time() - last_telemetry_time >= self.telemetry_interval_sec:
                    self.emit_telemetry_snapshot(cycle_count, latencies, last_tick_seen_msc)
                    last_telemetry_time = time.time()

                time.sleep(poll_sec)

        except Exception as e:
            logger.error(f"Erro no loop de streaming supervisionado: {e}", exc_info=True)
        finally:
            logger.info("Executando drenagem final de pendências e encerramento seguro...")
            try:
                # Drenar pendências remanescentes
                drained_total = 0
                while True:
                    d = self.engine.replicate_pending_to_postgres(batch_size=5000)
                    if d == 0:
                        break
                    drained_total += d
                logger.info(f"Drenagem final concluída: {drained_total} itens replicados.")
                
                # Sincronização final de candles M2
                final_m2 = self.engine.sync_m2_candles()
                logger.info(f"Sincronização final de candles M2: {final_m2} candles.")
            except Exception as e:
                logger.error(f"Erro na drenagem final: {e}")
            finally:
                self.engine.shutdown()
                end_wall_brt = datetime.now(timezone.utc).astimezone(self.engine.time_normalizer.market_tz)
                logger.info(f"Sessão supervisionada encerrada graciosamente às {end_wall_brt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                self.emit_telemetry_snapshot(cycle_count, latencies, last_tick_seen_msc)

if __name__ == "__main__":
    app = SupervisedSessionApp()
    app.run()
