import time
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Dict, Any, Optional

try:
    import zoneinfo
    BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")
except Exception:
    BRT_TZ = timezone(timedelta(hours=-3))

from src.config import AppConfig
from src.domain.models import Checkpoints
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.buffer.durable_spool import DurableSpool
from src.reconciliation.tick_reconciler import TickReconciler
from src.storage.postgres_writer import PostgresMarketWriter
from src.recovery.gap_recovery import GapRecoveryManager

logger = logging.getLogger(__name__)

class MarketCollectorEngine:
    """
    Orquestrador de alta integridade para coleta de dados de mercado (WINZ26).
    Implementa arquitetura de dois estágios:
    1. Captura Local Durável (SQLite WAL / synchronous=FULL).
    2. Replicação Remota no PostgreSQL 17 com confirmação de ACK.
    """

    def __init__(
        self,
        config: AppConfig,
        reader: Optional[MT5ReadOnlyReader] = None,
        spool: Optional[DurableSpool] = None,
        writer: Optional[PostgresMarketWriter] = None
    ):
        self.config = config
        self.symbol = config.symbol
        self.reader = reader or MT5ReadOnlyReader()
        self.spool = spool or DurableSpool(config.buffer.sqlite_path)
        self.writer = writer or PostgresMarketWriter(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password
        )
        self.reconciler = TickReconciler()
        self.gap_manager = GapRecoveryManager(
            reader=self.reader,
            reconciler=self.reconciler,
            inactivity_threshold_seconds=config.collector.gap_inactivity_threshold_seconds
        )
        self._is_running = False
        self.boot_time_msc: int = 0

    def _get_session_start_msc(self, target_date: Optional[date] = None) -> int:
        """
        Retorna o instante em milissegundos UTC do início da sessão de negociação (09:00 BRT).
        Para 19/10/2026 às 09:00 BRT -> 12:00 UTC (1792411200000 msc).
        Garante que a captura nunca inicie na época de 1970 na ausência de checkpoint.
        """
        now_utc = datetime.now(timezone.utc)
        if target_date is None:
            target_date = now_utc.astimezone(BRT_TZ).date()
        session_brt = datetime(target_date.year, target_date.month, target_date.day, 9, 0, 0, tzinfo=BRT_TZ)
        return int(session_brt.astimezone(timezone.utc).timestamp() * 1000)

    def initialize_and_recover(self) -> Dict[str, Any]:
        """
        Inicializa conexões e executa recuperação histórica inicial caso haja
        lapso temporal desde o início da sessão ou desde o último checkpoint de captura.
        Falha explicitamente com RuntimeError se o MT5 não puder ser inicializado ou se
        houver inconsistência/corrupção de checkpoint.
        """
        logger.info(f"Conectando ao terminal MT5 (Modo Somente Leitura) para o ativo {self.symbol}...")
        if not self.reader.initialize():
            self.shutdown()
            raise RuntimeError(f"Falha crítica na inicialização do MT5 para o símbolo {self.symbol}. Conexão não pôde ser estabelecida.")

        if not self.reader.is_connected():
            self.shutdown()
            raise RuntimeError(f"Terminal MT5 inicializado, porém desconectado do servidor da corretora para o símbolo {self.symbol}.")

        if not self.reader.select_symbol(self.symbol):
            self.shutdown()
            raise RuntimeError(f"Falha crítica: Símbolo {self.symbol} não pôde ser selecionado no MT5.")

        self.boot_time_msc = int(datetime.now(timezone.utc).timestamp() * 1000)
        ckp = self.spool.get_checkpoints(self.symbol)
        now_msc = self.boot_time_msc

        # 1. Validação estrita de integridade dos checkpoints
        if ckp.last_captured_msc < 0 or ckp.last_replicated_msc < 0:
            self.shutdown()
            raise RuntimeError(f"Inconsistência crítica: checkpoints com valores negativos (capturado={ckp.last_captured_msc}, replicado={ckp.last_replicated_msc}).")

        if ckp.last_replicated_msc > ckp.last_captured_msc:
            self.shutdown()
            raise RuntimeError(f"Inconsistência crítica: last_replicated_msc ({ckp.last_replicated_msc}) > last_captured_msc ({ckp.last_captured_msc}). Spool corrompido.")

        recovery_result = {"status": "INITIALIZED", "recovered_count": 0}

        # 2. Determinar ponto de partida inicial
        session_start_msc = self._get_session_start_msc()

        if ckp.last_captured_msc == 0:
            # Ausência legítima de checkpoint (primeiro boot frio)
            # Iniciar estritamente no início do pregão configurado (09:00 BRT / 12:00 UTC), NUNCA em 1970
            from_msc = session_start_msc
            logger.info(f"Primeiro boot sem checkpoint para {self.symbol}. Ancorando no início da sessão (09:00 BRT): {from_msc} msc...")
            
            if now_msc > from_msc:
                logger.info(f"Recuperando histórico de ticks desde o início da sessão ({from_msc} a {now_msc})...")
                from_utc = datetime.fromtimestamp(from_msc / 1000.0, tz=timezone.utc)
                to_utc = datetime.fromtimestamp(now_msc / 1000.0, tz=timezone.utc)
                raw_historical = self.reader.fetch_ticks_range(self.symbol, from_utc, to_utc)
                if raw_historical:
                    rec_ticks = self.reconciler.reconcile_raw_ticks(
                        raw_ticks=raw_historical,
                        symbol=self.symbol,
                        provenance="RECOVERED_HISTORICAL_MT5",
                        cutoff_realtime_msc=self.boot_time_msc
                    )
                    if rec_ticks:
                        self.spool.append_ticks_batch(rec_ticks, stream_key=self.symbol)
                        recovery_result["recovered_count"] = len(rec_ticks)
                        recovery_result["status"] = "RECOVERED_HISTORICAL_ON_STARTUP"
                else:
                    if self.reader.is_connected():
                        recovery_result["status"] = "CONFIRMED_ZERO_MARKET_VOLUME"
                    else:
                        recovery_result["status"] = "UNRECOVERED_GAP"
        else:
            # Checkpoint prévio existente: avaliar gap contextual
            logger.info(f"Retomando coleta a partir do checkpoint {ckp.last_captured_msc}...")
            res = self.gap_manager.evaluate_and_recover(
                symbol=self.symbol,
                last_seen_msc=ckp.last_captured_msc,
                current_time_msc=now_msc,
                is_connected=self.reader.is_connected()
            )
            if res.get("recovered_ticks"):
                rec_ticks = res["recovered_ticks"]
                self.spool.append_ticks_batch(rec_ticks, stream_key=self.symbol)
                recovery_result["recovered_count"] = len(rec_ticks)
                recovery_result["status"] = "RECOVERED_ON_STARTUP"

        # Drenar pendências que ficaram do último encerramento
        replicated = self.replicate_pending_to_postgres()
        recovery_result["drained_pending_count"] = replicated

        self._is_running = True
        return recovery_result

    def shutdown(self) -> None:
        """
        Encerramento seguro do engine e liberação explícita da conexão com o MT5.
        """
        self._is_running = False
        try:
            self.reader.shutdown()
            logger.info("Conexão com MT5 encerrada com segurança.")
        except Exception as e:
            logger.warning(f"Erro ao encerrar conexão MT5: {e}")

    def _attempt_mt5_reconnect(self, max_attempts: int = 3, base_backoff_sec: float = 0.5) -> bool:
        """
        Executa rotina de reconexão controlada com backoff exponencial limitado.
        """
        logger.info("Iniciando rotina de reconexão controlada ao MT5...")
        for attempt in range(1, max_attempts + 1):
            sleep_time = base_backoff_sec * (2 ** (attempt - 1))
            time.sleep(sleep_time)
            try:
                self.reader.shutdown()
                if self.reader.initialize() and self.reader.is_connected() and self.reader.select_symbol(self.symbol):
                    logger.info(f"Reconexão ao MT5 restabelecida com sucesso na tentativa {attempt}.")
                    return True
            except Exception as e:
                logger.warning(f"Tentativa {attempt} de reconexão falhou: {e}")
        logger.error(f"Esgotadas {max_attempts} tentativas de reconexão ao MT5.")
        return False

    def capture_step(self, raw_ticks: Optional[list] = None) -> int:
        """
        Estágio 1: Captura e grava duravelmente no buffer local SQLite WAL.
        Avança last_captured_msc.
        Diferencia ausência legítima de negócios de desconexão do MT5.
        """
        if raw_ticks is None:
            # Verificar conectividade ativa do MT5 antes da consulta
            if not self.reader.is_connected():
                logger.warning(f"Desconexão do terminal MT5 detectada durante captura de {self.symbol}.")
                reconnected = self._attempt_mt5_reconnect()
                if not reconnected:
                    return 0

            ckp = self.spool.get_checkpoints(self.symbol)
            if ckp.last_captured_msc > 0:
                from_msc = ckp.last_captured_msc
            else:
                from_msc = self._get_session_start_msc()

            from_utc = datetime.fromtimestamp(from_msc / 1000.0, tz=timezone.utc)
            raw_ticks = self.reader.fetch_ticks_from(self.symbol, from_utc, count=self.config.buffer.batch_size)

        if not raw_ticks:
            return 0

        # Reconciliar por multiplicidade de ocorrências e filtrar negócios
        cutoff = self.boot_time_msc if self.boot_time_msc > 0 else None
        reconciled = self.reconciler.reconcile_raw_ticks(
            raw_ticks=raw_ticks,
            symbol=self.symbol,
            provenance="REALTIME_STREAM",
            cutoff_realtime_msc=cutoff
        )

        if not reconciled:
            return 0

        # Gravar no SQLite WAL e avançar last_captured_msc
        inserted = self.spool.append_ticks_batch(reconciled, stream_key=self.symbol)
        return inserted

    def replicate_pending_to_postgres(self, batch_size: int = 1000) -> int:
        """
        Estágio 2: Lê ticks pendentes do buffer local e replica no PostgreSQL.
        Avança last_replicated_msc somente após o ACK de persistência remota.
        """
        pending = self.spool.get_pending_ticks(limit=batch_size)
        if not pending:
            return 0

        try:
            self.writer.write_ticks_batch(pending)
            fps = [t.tick_fingerprint for t in pending]
            max_msc = max(t.time_msc for t in pending)
            self.spool.mark_ticks_replicated(fps, max_msc, stream_key=self.symbol)
            return len(pending)
        except Exception as e:
            logger.warning(f"PostgreSQL temporariamente indisponível ({e}). Itens permanecem no spool local durável.")
            return 0
