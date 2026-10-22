import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from src.domain.models import TradeTick
from src.connection.mt5_reader import MT5ReadOnlyReader

from src.buffer.durable_spool import DurableSpool
from src.reconciliation.tick_reconciler import TickReconciler
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

logger = logging.getLogger("PeriodicHistoricalReconciler")

class PeriodicHistoricalReconciler:
    """
    Reconciliador histórico periódico independente.
    Executa varreduras históricas retrospectivas em segundo plano mesmo sob conexão contínua e estável,
    detectando e inserindo negócios disponibilizados tardiamente pelo MT5 (atraso > lookback_ms).
    
    Mantém separação estrita de watermarks:
    - W_durable (last_captured_msc): avança no realtime stream de forma estritamente crescente.
    - W_complete (watermark_complete_msc): avança retrospectivamente apenas até onde a reconciliação
      histórica comprovou ausência de gaps, respeitando o atraso de segurança (safety_lag_ms).
    """

    def __init__(
        self,
        reader: MT5ReadOnlyReader,
        spool: DurableSpool,
        symbol: str,
        safety_lag_ms: int = 2000,
        max_window_ms: int = 300_000, # 5 minutos
        time_normalizer: Optional[MT5TimeNormalizer] = None
    ):
        self.reader = reader
        self.spool = spool
        self.symbol = symbol
        self.safety_lag_ms = safety_lag_ms
        self.max_window_ms = max_window_ms
        self.time_normalizer = time_normalizer or MT5TimeNormalizer()

    def reconcile_cycle(
        self,
        now_server_msc: int,
        session_start_msc: int = 0,
        replicate_fn: Optional[Any] = None
    ) -> Dict[str, Any]:

        """
        Executa um ciclo determinístico de reconciliação periódica.
        Retorna dicionário com o resultado e métricas da varredura.
        """
        if not self.reader.is_connected():
            return {
                "status": "SKIPPED_DISCONNECTED",
                "recovered_count": 0,
                "reason": "MT5 desconectado"
            }

        ckp = self.spool.get_checkpoints(self.symbol)
        
        # H002 B.2 Correção P0-02: Limite superior seguro baseado exclusivamente no relógio estável do mercado
        # Desacoplado estritamente de last_captured_msc.
        audit_ceiling_msc = now_server_msc - self.safety_lag_ms

        # Ponto inicial da janela de reconciliação a partir de cursor próprio e persistente
        if ckp.watermark_complete_msc > 0:
            from_msc = ckp.watermark_complete_msc
        elif ckp.last_captured_msc > 0:
            # Sem watermark prévio: iniciar na janela retrospectiva recente (ex: últimos 5 min)
            base_start = session_start_msc if session_start_msc > 0 else 0
            from_msc = max(base_start, audit_ceiling_msc - self.max_window_ms)
        elif session_start_msc > 0:
            from_msc = session_start_msc
        else:
            return {
                "status": "SKIPPED_NO_BASE_POINT",
                "recovered_count": 0,
                "reason": "Sem ponto base de partida para auditoria"
            }

        if audit_ceiling_msc <= from_msc:
            # Impedir que lacunas pendentes sejam interpretadas como UP_TO_DATE
            oldest_pending = self.spool.get_oldest_pending_msc(self.symbol)
            if oldest_pending is not None and oldest_pending <= audit_ceiling_msc:
                return {
                    "status": "PENDING_REPLICATION_OPEN",
                    "recovered_count": 0,
                    "w_complete": ckp.watermark_complete_msc,
                    "w_durable": ckp.last_captured_msc,
                    "reason": "Existem registros aguardando confirmação de durabilidade no PostgreSQL"
                }
            return {
                "status": "UP_TO_DATE",
                "recovered_count": 0,
                "w_complete": ckp.watermark_complete_msc,
                "w_durable": ckp.last_captured_msc
            }

        # Delimitar janela máxima segura
        to_msc = min(from_msc + self.max_window_ms, audit_ceiling_msc)

        # Consultar histórico do MT5 com contrato formal de status
        query_res = None
        if hasattr(self.reader, "query_ticks_range"):
            query_res = self.reader.query_ticks_range(self.symbol, from_msc, to_msc)

        from src.connection.mt5_reader import MT5QueryResult, MT5QueryStatus
        if isinstance(query_res, MT5QueryResult):
            if query_res.status == MT5QueryStatus.DISCONNECTED:
                return {
                    "status": "SKIPPED_DISCONNECTED",
                    "recovered_count": 0,
                    "reason": "MT5 desconectado durante consulta histórica"
                }
            if query_res.status in (MT5QueryStatus.MT5_QUERY_ERROR, MT5QueryStatus.INCONCLUSIVE):
                logger.warning(
                    f"Janela [{from_msc}, {to_msc}] INCONCLUSIVA: status={query_res.status}, "
                    f"code={query_res.error_code}, msg={query_res.error_message}. W_complete RETIDO."
                )
                return {
                    "status": "INCONCLUSIVE",
                    "from_msc": from_msc,
                    "to_msc": to_msc,
                    "recovered_count": 0,
                    "w_complete": ckp.watermark_complete_msc,
                    "w_durable": ckp.last_captured_msc,
                    "reason": f"MT5 status {query_res.status}: {query_res.error_message}"
                }
            if query_res.status == MT5QueryStatus.SUCCESS_EMPTY_VERIFIED:
                raw_ticks = []
            else:
                raw_ticks = query_res.ticks
        else:
            # Fallback para mocks legados ou implementações que expõem apenas fetch_ticks_range
            raw_ticks = self.reader.fetch_ticks_range(self.symbol, from_msc, to_msc)
            if not isinstance(raw_ticks, list) or not self.reader.is_connected():
                logger.warning(
                    f"Janela [{from_msc}, {to_msc}] INCONCLUSIVA: dados históricos do MT5 indisponíveis. "
                    "W_complete NÃO avançará sobre lacuna não verificada."
                )
                return {
                    "status": "INCONCLUSIVE",
                    "from_msc": from_msc,
                    "to_msc": to_msc,
                    "recovered_count": 0,
                    "w_complete": ckp.watermark_complete_msc,
                    "w_durable": ckp.last_captured_msc,
                    "reason": "Dados históricos indisponíveis ou terminal desconectado no MT5"
                }

        # H002 B.2 Condição 8: Não presumir completude ilimitada se a janela foi truncada
        audited_to_msc = to_msc
        is_saturated = isinstance(query_res, MT5QueryResult) and bool(query_res.is_capacity_saturated)
        if isinstance(raw_ticks, list) and len(raw_ticks) > 0 and is_saturated:
            max_ret_msc = max(t["time_msc"] for t in raw_ticks)
            sub_mscs = [t["time_msc"] for t in raw_ticks if t["time_msc"] < max_ret_msc]
            if sub_mscs:
                audited_to_msc = max(sub_mscs)
                raw_ticks = [t for t in raw_ticks if t["time_msc"] <= audited_to_msc]
            else:
                audited_to_msc = from_msc
                raw_ticks = []

        recovered_count = 0
        if raw_ticks:
            # Reconciliar ticks brutos do MT5
            reconciler = TickReconciler(time_normalizer=self.time_normalizer)
            canonical_ticks = reconciler.reconcile_raw_ticks(
                raw_ticks=raw_ticks,
                symbol=self.symbol,
                provenance="RECOVERED_HISTORICAL_MT5"
            )

            # Identificar existentes no spool local
            existing_fps = self.spool.get_existing_fingerprints_in_range(from_msc, audited_to_msc, self.symbol)
            missing_ticks = [t for t in canonical_ticks if t.tick_fingerprint not in existing_fps]

            if missing_ticks:
                # Inserir no buffer local durável SEM regredir nem avançar last_captured_msc
                recovered_count = self.spool.append_recovered_ticks(missing_ticks, stream_key=self.symbol)
                logger.info(
                    f"Reconciliador Periódico recuperou {recovered_count} negócios tardios em {self.symbol} "
                    f"no intervalo [{from_msc}, {audited_to_msc}]."
                )

        # Se houver função de replicação fornecida, replica pendências para o PostgreSQL imediatamente:
        # acionado tanto para novos registros recuperados quanto para pendências prévias não confirmadas (recuperação pós-crash)
        if replicate_fn is not None and (recovered_count > 0 or self.spool.has_pending_ticks_in_range(from_msc, audited_to_msc, self.symbol)):
            try:
                replicate_fn()
            except Exception as e:
                logger.warning(f"Falha na replicação imediata de recuperados para o PostgreSQL: {e}")

        # Condição Estrita de Avanço de W_complete:
        # Só avança se NÃO houver pendências (status PENDING) no intervalo auditado.
        has_pending = self.spool.has_pending_ticks_in_range(from_msc, audited_to_msc, self.symbol)
        if has_pending:
            logger.warning(
                f"W_complete RETIDO: Existem negócios com status PENDING no intervalo [{from_msc}, {audited_to_msc}]. "
                "W_complete não avançará até que todos sejam confirmados (COMMITTED) no PostgreSQL."
            )
            return {
                "status": "RECOVERED_PENDING_REPLICATION",
                "from_msc": from_msc,
                "to_msc": audited_to_msc,
                "scanned_ticks": len(raw_ticks) if raw_ticks else 0,
                "recovered_count": recovered_count,
                "w_complete": ckp.watermark_complete_msc,
                "w_durable": ckp.last_captured_msc,
                "pending_replication": True
            }

        # Avançar W_complete estritamente até audited_to_msc somente quando a cobertura histórica estiver verificada
        # E todos os negócios estiverem efetivamente confirmados no PostgreSQL.
        if audited_to_msc > ckp.watermark_complete_msc:
            self.spool.update_complete_watermark(audited_to_msc, stream_key=self.symbol)

        logger.info(
            f"Reconciliação periódica auditada [{from_msc}, {audited_to_msc}]: {len(raw_ticks) if raw_ticks else 0} ticks verificados, "
            f"{recovered_count} recuperados. W_complete avançou para {audited_to_msc}."
        )

        return {
            "status": "RECONCILED",
            "from_msc": from_msc,
            "to_msc": audited_to_msc,
            "scanned_ticks": len(raw_ticks) if raw_ticks else 0,
            "recovered_count": recovered_count,
            "w_complete": audited_to_msc,
            "w_durable": ckp.last_captured_msc,
            "pending_replication": False
        }


