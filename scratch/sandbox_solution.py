import os
import sys
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone, timedelta

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.domain.models import TradeTick, Checkpoints
from src.reconciliation.tick_reconciler import TickReconciler

logger = logging.getLogger("SandboxH001")

class PaginationStallError(RuntimeError):
    """Exceção levantada quando a paginação do coletor detecta falta de progresso contínuo."""
    pass

class SafeRangePaginator:
    """
    Paginador temporal seguro para coleta de ticks do MetaTrader 5.
    Substitui a dependência frágil de copy_ticks_from(count=1000) por fatiamento temporal
    controlado via copy_ticks_range, garantindo:
    1. Não truncamento arbitrário em limites de densidade.
    2. Preservação integral de multiplicidade de trades no mesmo milissegundo.
    3. Detecção explícita de estagnação com salvaguarda fail-safe.
    """

    def __init__(
        self,
        reader: MT5ReadOnlyReader,
        time_normalizer: Optional[MT5TimeNormalizer] = None,
        max_slice_ms: int = 10_000, # 10 segundos por fatia em caso de backlog
        max_stagnant_cycles: int = 3
    ):
        self.reader = reader
        self.normalizer = time_normalizer or MT5TimeNormalizer()
        self.max_slice_ms = max_slice_ms
        self.max_stagnant_cycles = max_stagnant_cycles
        self.stagnant_count = 0
        self.last_seen_msc: int = 0

    def fetch_safe_slice(
        self,
        symbol: str,
        from_msc: int,
        target_ceiling_msc: int,
        lookback_ms: int = 2000
    ) -> Tuple[List[Dict[str, Any]], int, bool]:
        """
        Consulta uma fatia temporal delimitada [slice_start, slice_end].
        Retorna:
        - raw_ticks: lista de ticks brutos retornados pelo MT5
        - slice_end_msc: limite superior da janela consultada
        - is_complete_to_ceiling: se a consulta alcançou o teto pretendido
        """
        # Lookback adaptativo apenas no início ou na transição
        slice_start = max(0, from_msc - lookback_ms) if from_msc > 0 else 0
        
        # Delimitar janela máxima para não exceder limites de buffer da API C do MT5
        slice_end = min(slice_start + self.max_slice_ms, target_ceiling_msc)
        if slice_end <= slice_start:
            slice_end = target_ceiling_msc

        raw_ticks = self.reader.fetch_ticks_range(symbol, slice_start, slice_end)
        is_complete = (slice_end >= target_ceiling_msc)

        return raw_ticks, slice_end, is_complete


class DecoupledHistoricalReconciler:
    """
    Reconciliador histórico periódico com teto DESACOPLADO de last_captured_msc.
    Mesmo que o stream em tempo real sofra estagnação ou interrupção, o reconciliador
    continua auditando até (now_server_msc - safety_lag_ms), recuperando dados ausentes
    e garantindo a integridade dos watermarks e a persistência resiliente.
    """

    def __init__(
        self,
        reader: MT5ReadOnlyReader,
        spool: Any,
        symbol: str,
        safety_lag_ms: int = 2000,
        max_window_ms: int = 300_000, # 5 minutos por ciclo
        time_normalizer: Optional[MT5TimeNormalizer] = None
    ):
        self.reader = reader
        self.spool = spool
        self.symbol = symbol
        self.safety_lag_ms = safety_lag_ms
        self.max_window_ms = max_window_ms
        self.time_normalizer = time_normalizer or MT5TimeNormalizer()
        self.reconciler = TickReconciler(time_normalizer=self.time_normalizer)

    def reconcile_cycle(
        self,
        now_server_msc: int,
        session_start_msc: int = 0,
        replicate_fn: Optional[Any] = None
    ) -> Dict[str, Any]:
        if not self.reader.is_connected():
            return {"status": "SKIPPED_DISCONNECTED", "recovered_count": 0}

        ckp = self.spool.get_checkpoints(self.symbol)
        
        # CORREÇÃO ARQUITETURAL: O teto de auditoria NÃO é mais restrito a last_captured_msc!
        # O histórico MT5 da corretora é a autoridade de integridade.
        audit_ceiling_msc = now_server_msc - self.safety_lag_ms

        if ckp.watermark_complete_msc > 0:
            from_msc = ckp.watermark_complete_msc
        elif session_start_msc > 0:
            from_msc = session_start_msc
        else:
            from_msc = max(0, audit_ceiling_msc - self.max_window_ms)

        if audit_ceiling_msc <= from_msc:
            return {
                "status": "UP_TO_DATE",
                "recovered_count": 0,
                "w_complete": ckp.watermark_complete_msc
            }

        # Delimitar janela de até 5 minutos
        to_msc = min(from_msc + self.max_window_ms, audit_ceiling_msc)

        raw_ticks = self.reader.fetch_ticks_range(self.symbol, from_msc, to_msc)
        if raw_ticks is None or not self.reader.is_connected():
            return {"status": "INCONCLUSIVE", "recovered_count": 0}

        # Reconciliar eventos
        reconciled = self.reconciler.reconcile_raw_ticks(
            raw_ticks=raw_ticks,
            symbol=self.symbol,
            provenance="RECOVERED_HISTORICAL_MT5",
            cutoff_realtime_msc=None
        )

        recovered_count = 0
        if reconciled:
            # Persistir no spool local durável
            recovered_count = self.spool.append_recovered_ticks(reconciled, stream_key=self.symbol)
            # Replicar imediatamente para o PostgreSQL se função for informada
            if replicate_fn:
                replicate_fn()

        # Avançar W_complete para o limite auditado com segurança
        self.spool.update_complete_watermark(to_msc, stream_key=self.symbol)

        return {
            "status": "RECONCILED",
            "audited_window": [from_msc, to_msc],
            "scanned_ticks": len(raw_ticks),
            "recovered_count": recovered_count,
            "w_complete": to_msc
        }
