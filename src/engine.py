import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
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

    def initialize_and_recover(self) -> Dict[str, Any]:
        """
        Inicializa conexões e executa recuperação histórica inicial caso haja
        lapso temporal desde o último checkpoint de captura.
        """
        ckp = self.spool.get_checkpoints(self.symbol)
        now_msc = int(datetime.now(timezone.utc).timestamp() * 1000)

        recovery_result = {"status": "INITIALIZED", "recovered_count": 0}

        if ckp.last_captured_msc > 0:
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

    def capture_step(self, raw_ticks: Optional[list] = None) -> int:
        """
        Estágio 1: Captura e grava duravelmente no buffer local SQLite WAL.
        Avança last_captured_msc.
        """
        if raw_ticks is None:
            # Consulta MT5 real a partir do último cursor de captura
            ckp = self.spool.get_checkpoints(self.symbol)
            from_utc = datetime.fromtimestamp(max(ckp.last_captured_msc, 0) / 1000.0, tz=timezone.utc)
            raw_ticks = self.reader.fetch_ticks_from(self.symbol, from_utc, count=self.config.buffer.batch_size)

        if not raw_ticks:
            return 0

        # Reconciliar por multiplicidade de ocorrências e assinar determinística
        reconciled = self.reconciler.reconcile_raw_ticks(
            raw_ticks=raw_ticks,
            symbol=self.symbol,
            provenance="REALTIME_STREAM"
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
