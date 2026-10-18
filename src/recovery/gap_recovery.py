from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from src.domain.models import TradeTick, QualityEvent
from src.domain.calendar import is_b3_trading_day
from src.reconciliation.tick_reconciler import TickReconciler
from src.connection.mt5_reader import MT5ReadOnlyReader

class GapRecoveryManager:
    """
    Gerencia a detecção contextual de lacunas e a recuperação histórica via MT5
    com sobreposição de intervalos e reconciliação por multiplicidade.
    """

    def __init__(
        self,
        reader: MT5ReadOnlyReader,
        reconciler: TickReconciler,
        inactivity_threshold_seconds: int = 15
    ):
        self.reader = reader
        self.reconciler = reconciler
        self.inactivity_threshold_seconds = inactivity_threshold_seconds

    def evaluate_and_recover(
        self,
        symbol: str,
        last_seen_msc: int,
        current_time_msc: int,
        is_connected: bool
    ) -> Dict[str, Any]:
        """
        Avalia se a lacuna temporal decorre de inatividade natural de mercado
        ou desconexão, disparando a recuperação histórica paginada caso necessário.
        """
        gap_ms = current_time_msc - last_seen_msc
        if gap_ms <= 0:
            return {"status": "NO_GAP", "recovered_ticks": []}

        gap_seconds = gap_ms / 1000.0
        from_utc = datetime.fromtimestamp(last_seen_msc / 1000.0, tz=timezone.utc)
        to_utc = datetime.fromtimestamp(current_time_msc / 1000.0, tz=timezone.utc)

        # 1. Checar se está dentro do dia útil B3
        trading_day = is_b3_trading_day(from_utc.date())

        # Se for um intervalo curto (< threshold) e conexão ativa, é inatividade natural
        if gap_seconds < self.inactivity_threshold_seconds and is_connected and trading_day:
            return {
                "status": "NATURAL_MARKET_INACTIVITY",
                "gap_seconds": gap_seconds,
                "recovered_ticks": []
            }

        # 2. Consultar MT5 para o intervalo sobreposto [from_utc, to_utc]
        raw_historical = self.reader.fetch_ticks_range(symbol, from_utc, to_utc)

        # Se o MT5 retornar vazio e a conexão estiver ativa, o broker confirma que não houve negócios
        if not raw_historical:
            if is_connected:
                return {
                    "status": "CONFIRMED_ZERO_MARKET_VOLUME",
                    "gap_seconds": gap_seconds,
                    "recovered_ticks": []
                }
            else:
                # Conexão estava inativa e histórico retornou vazio: incerteza de completude
                quality_event = QualityEvent(
                    event_time_utc=datetime.now(timezone.utc),
                    event_type="DISCONNECTION_DATA_UNCERTAIN",
                    symbol=symbol,
                    gap_start_utc=from_utc,
                    gap_end_utc=to_utc,
                    ticks_recovered=0,
                    broker_status="DISCONNECTED",
                    provenance_uncertainty=True,
                    description="Desconexão detectada e corretora não retornou histórico para a lacuna."
                )
                return {
                    "status": "UNRECOVERED_GAP",
                    "quality_event": quality_event,
                    "recovered_ticks": []
                }

        # 3. Reconciliar dados recuperados pelo reconciliador de multiplicidade
        recovered_ticks = self.reconciler.reconcile_raw_ticks(
            raw_ticks=raw_historical,
            symbol=symbol,
            provenance="RECOVERED_HISTORICAL_MT5"
        )

        quality_event = QualityEvent(
            event_time_utc=datetime.now(timezone.utc),
            event_type="GAP_RECOVERED",
            symbol=symbol,
            gap_start_utc=from_utc,
            gap_end_utc=to_utc,
            ticks_recovered=len(recovered_ticks),
            broker_status="RECONNECTED",
            provenance_uncertainty=False,
            description=f"Recuperados {len(recovered_ticks)} ticks históricos da corretora."
        )

        return {
            "status": "RECOVERED",
            "quality_event": quality_event,
            "recovered_ticks": recovered_ticks
        }
