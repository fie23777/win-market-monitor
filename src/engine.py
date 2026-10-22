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
from src.domain.models import Checkpoints, M2CandleClosed
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader, MT5QueryStatus, MT5QueryResult
from src.buffer.durable_spool import DurableSpool
from src.reconciliation.tick_reconciler import TickReconciler
from src.reconciliation.periodic_reconciler import PeriodicHistoricalReconciler
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
        writer: Optional[PostgresMarketWriter] = None,
        time_normalizer: Optional[MT5TimeNormalizer] = None
    ):
        self.config = config
        self.symbol = config.symbol
        
        server_tz = config.timezones.get("mt5_server", "America/Sao_Paulo") if hasattr(config, "timezones") and isinstance(config.timezones, dict) else "America/Sao_Paulo"
        market_tz = config.timezones.get("market", "America/Sao_Paulo") if hasattr(config, "timezones") and isinstance(config.timezones, dict) else "America/Sao_Paulo"
        self.time_normalizer = time_normalizer or MT5TimeNormalizer(server_timezone=server_tz, market_timezone=market_tz)

        self.reader = reader or MT5ReadOnlyReader(time_normalizer=self.time_normalizer)
        self.spool = spool or DurableSpool(config.buffer.sqlite_path, time_normalizer=self.time_normalizer)
        self.writer = writer or PostgresMarketWriter(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password
        )
        self.reconciler = TickReconciler(time_normalizer=self.time_normalizer)
        self.gap_manager = GapRecoveryManager(
            reader=self.reader,
            reconciler=self.reconciler,
            inactivity_threshold_seconds=config.collector.gap_inactivity_threshold_seconds
        )
        self.periodic_reconciler = PeriodicHistoricalReconciler(
            reader=self.reader,
            spool=self.spool,
            symbol=self.symbol,
            safety_lag_ms=getattr(self.config.collector, "lookback_ms", 2000),
            max_window_ms=300_000,
            time_normalizer=self.time_normalizer
        )
        self._is_running = False
        self.boot_time_msc: int = 0
        self._adaptive_batch_size: int = self.config.buffer.batch_size
        self._cursor_stagnation_count: int = 0
        self._density_stagnation_count: int = 0
        self._last_cursor_from_msc: int = 0
        self._last_batch_saturated: bool = False
        self._last_safe_msc_advanced: bool = False
        self._stagnation_limit_cycles: int = getattr(self.config.collector, "stagnation_limit_cycles", 5)
        self._max_batch_size: int = getattr(self.config.collector, "max_batch_size", 5000)

        # Estado operacional e telemetria acionável
        self.operational_status: str = "HEALTHY"
        self.has_critical_stagnation: bool = False
        self.last_stagnation_error: Optional[str] = None
        self.inconclusive_intervals: list = []

    def is_healthy(self) -> bool:
        """Indica se o coletor está em estado operacional saudável ou sob contenção fail-closed."""
        return self.operational_status == "HEALTHY" and not self.has_critical_stagnation

    def _get_session_start_msc(self, target_date: Optional[date] = None) -> int:
        """
        Retorna o instante em milissegundos no referencial do servidor MT5
        para o início da sessão de negociação B3 (09:00 BRT).
        Para 19/10/2026 às 09:00 BRT -> 1792400400000 msc (servidor XP).
        Garante que a captura nunca inicie na época de 1970 na ausência de checkpoint.
        """
        return self.time_normalizer.get_session_start_server_msc(target_date, hour=9, minute=0)

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

        now_utc = datetime.now(timezone.utc)
        self.boot_time_msc = self.time_normalizer.utc_datetime_to_server_msc(now_utc)
        ckp = self.spool.get_checkpoints(self.symbol)
        now_msc = self.boot_time_msc

        # 1. Validação estrita de integridade dos checkpoints
        if ckp.last_captured_msc < 0 or ckp.last_replicated_msc < 0:
            self.shutdown()
            raise RuntimeError(f"Inconsistência crítica: checkpoints com valores negativos (capturado={ckp.last_captured_msc}, replicado={ckp.last_replicated_msc}).")

        if ckp.last_replicated_msc > ckp.last_captured_msc:
            self.shutdown()
            raise RuntimeError(f"Inconsistência crítica: last_replicated_msc ({ckp.last_replicated_msc}) > last_captured_msc ({ckp.last_captured_msc}). Spool corrompido.")

        recovery_result = {"status": "INITIALIZED", "recovered_count": 0, "candles_m2_count": 0}

        # 2. Determinar ponto de partida inicial
        session_start_msc = self._get_session_start_msc()

        if ckp.last_captured_msc == 0:
            # Ausência legítima de checkpoint (primeiro boot frio)
            # Iniciar estritamente no início do pregão configurado (09:00 BRT / 12:00 UTC), NUNCA em 1970
            from_msc = session_start_msc
            logger.info(f"Primeiro boot sem checkpoint para {self.symbol}. Ancorando no início da sessão (09:00 BRT): {from_msc} msc...")
            
            if now_msc > from_msc:
                logger.info(f"Recuperando histórico de ticks desde o início da sessão ({from_msc} a {now_msc})...")
                raw_historical = self.reader.fetch_ticks_range(self.symbol, from_msc, now_msc)
                if raw_historical:
                    rec_ticks = self.reconciler.reconcile_raw_ticks(
                        raw_ticks=raw_historical,
                        symbol=self.symbol,
                        provenance="RECOVERED_HISTORICAL_MT5",
                        cutoff_realtime_msc=self.boot_time_msc
                    )
                    if rec_ticks:
                        # Inserir no spool em lotes controlados
                        chunk_size = 10000
                        for i in range(0, len(rec_ticks), chunk_size):
                            chunk = rec_ticks[i:i + chunk_size]
                            self.spool.append_ticks_batch(chunk, stream_key=self.symbol)
                        recovery_result["recovered_count"] = len(rec_ticks)
                        recovery_result["status"] = "RECOVERED_HISTORICAL_ON_STARTUP"
                else:
                    if self.reader.is_connected():
                        recovery_result["status"] = "CONFIRMED_ZERO_MARKET_VOLUME"
                    else:
                        recovery_result["status"] = "UNRECOVERED_GAP"
        else:
            # Checkpoint prévio existente: avaliar se pertence ao mesmo pregão ou a sessão anterior
            ckp_dt = self.time_normalizer.server_msc_to_utc_datetime(ckp.last_captured_msc)
            ckp_date = ckp_dt.astimezone(self.time_normalizer.market_tz).date()
            today_date = datetime.now(timezone.utc).astimezone(self.time_normalizer.market_tz).date()

            if ckp_date < today_date:
                # Transição legítima de pregão: ancorar estritamente no início do dia de hoje (09:00 BRT)
                logger.info(
                    f"Transição de sessão detectada: checkpoint prévio ({ckp_date}) pertence a pregão anterior. "
                    f"Ancorando nova sessão de {today_date} no início oficial (09:00 BRT: {session_start_msc} msc)..."
                )
                from_msc = session_start_msc
                if now_msc > from_msc:
                    logger.info(f"Recuperando histórico de ticks desde o início da nova sessão ({from_msc} a {now_msc})...")
                    raw_historical = self.reader.fetch_ticks_range(self.symbol, from_msc, now_msc)
                    if raw_historical:
                        rec_ticks = self.reconciler.reconcile_raw_ticks(
                            raw_ticks=raw_historical,
                            symbol=self.symbol,
                            provenance="RECOVERED_HISTORICAL_MT5",
                            cutoff_realtime_msc=self.boot_time_msc
                        )
                        if rec_ticks:
                            chunk_size = 10000
                            for i in range(0, len(rec_ticks), chunk_size):
                                chunk = rec_ticks[i:i + chunk_size]
                                self.spool.append_ticks_batch(chunk, stream_key=self.symbol)
                            recovery_result["recovered_count"] = len(rec_ticks)
                            recovery_result["status"] = "RECOVERED_NEW_SESSION_ON_STARTUP"
                else:
                    logger.info(f"Boot pré-abertura (antes das 09:00 BRT). Estado: PRE_MARKET_STANDBY.")
                    recovery_result["status"] = "PRE_MARKET_STANDBY"
            else:
                # Checkpoint pertence à mesma sessão: avaliar gap contextual intra-dia
                logger.info(f"Retomando coleta intra-dia a partir do checkpoint {ckp.last_captured_msc}...")
                res = self.gap_manager.evaluate_and_recover(
                    symbol=self.symbol,
                    last_seen_msc=ckp.last_captured_msc,
                    current_time_msc=now_msc,
                    is_connected=self.reader.is_connected()
                )
                if res.get("recovered_ticks"):
                    rec_ticks = res["recovered_ticks"]
                    chunk_size = 10000
                    for i in range(0, len(rec_ticks), chunk_size):
                        chunk = rec_ticks[i:i + chunk_size]
                        self.spool.append_ticks_batch(chunk, stream_key=self.symbol)
                    recovery_result["recovered_count"] = len(rec_ticks)
                    recovery_result["status"] = "RECOVERED_ON_STARTUP"


        # Sincronizar candles M2 fechados
        try:
            m2_count = self.sync_m2_candles()
            recovery_result["candles_m2_count"] = m2_count
        except Exception as e:
            logger.warning(f"Aviso na sincronização de candles M2: {e}")

        # Drenar pendências para o PostgreSQL em lotes controlados
        replicated_total = 0
        drain_chunk_size = 5000
        while True:
            rep = self.replicate_pending_to_postgres(batch_size=drain_chunk_size)
            if rep == 0:
                break
            replicated_total += rep
        recovery_result["drained_pending_count"] = replicated_total

        self._is_running = True
        return recovery_result

    def sync_m2_candles(self) -> int:
        """
        Sincroniza candles M2 fechados do MT5 para o dia de hoje,
        convertendo os limites temporais via MT5TimeNormalizer e persistindo no PostgreSQL.
        """
        target_date = datetime.now(timezone.utc).astimezone(self.time_normalizer.market_tz).date()
        session_start_msc = self._get_session_start_msc(target_date)
        now_server_msc = self.time_normalizer.utc_datetime_to_server_msc(datetime.now(timezone.utc))

        rates = self.reader.fetch_rates_range(self.symbol, 2, session_start_msc, now_server_msc) # 2 = TIMEFRAME_M2
        if not rates:
            return 0

        synced = 0
        now_server_sec = now_server_msc // 1000
        for r in rates:
            candle_sec = int(r["time"])
            # Considerar fechado se o tempo de encerramento (candle_sec + 120s) for <= now_server_sec
            if candle_sec + 120 <= now_server_sec:
                open_utc = self.time_normalizer.server_seconds_to_utc_datetime(candle_sec)
                close_utc = open_utc + timedelta(minutes=2)
                candle = M2CandleClosed(
                    symbol=self.symbol,
                    open_time_utc=open_utc,
                    close_time_utc=close_utc,
                    open=r["open"],
                    high=r["high"],
                    low=r["low"],
                    close=r["close"],
                    volume_real=r["real_volume"],
                    tick_volume=r["tick_volume"],
                    spread=r["spread"],
                    is_closed=True,
                    provenance="RECOVERED_HISTORICAL_MT5"
                )
                self.spool.append_candle_m2(candle)
                self.writer.write_candle_m2(candle)
                synced += 1
        return synced

    def maybe_sync_m2_candles(self, last_sync_timestamp: float, interval_sec: float = 60.0) -> tuple[float, int]:
        """
        Sincroniza candles M2 se tiver decorrido o intervalo estipulado.
        """
        now = time.time()
        if now - last_sync_timestamp >= interval_sec:
            try:
                count = self.sync_m2_candles()
                return now, count
            except Exception as e:
                logger.warning(f"Aviso ao sincronizar candles M2: {e}")
                return now, 0
        return last_sync_timestamp, 0

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
        Avança last_captured_msc estritamente por cobertura temporal comprovada.
        Diferencia ausência legítima de negócios de erros e desconexão do MT5.
        Trata lotes densos com recuo preventivo da fronteira saturada.
        """
        if self.has_critical_stagnation:
            logger.error(
                f"Captura suspensa em fail-closed para {self.symbol}: {self.last_stagnation_error}"
            )
            return 0

        is_capacity_saturated = False
        ckp = self.spool.get_checkpoints(self.symbol)
        from_msc = 0

        if raw_ticks is None:
            # 1. Verificar conectividade ativa do MT5 antes da consulta
            if not self.reader.is_connected():
                logger.warning(f"Desconexão do terminal MT5 detectada durante captura de {self.symbol}.")
                reconnected = self._attempt_mt5_reconnect()
                if not reconnected:
                    return 0

            session_start = self._get_session_start_msc()
            lookback_ms = getattr(self.config.collector, "lookback_ms", 2000)
            
            if ckp.last_captured_msc > 0:
                if self._last_batch_saturated:
                    # Durante drenagem de lote saturado: se o lote anterior avançou com fronteira comprovada,
                    # avança para o próximo ms (+1) para não reprocessar ticks já fechados.
                    # Se estagnou (mono-temporal), mantém ckp.last_captured_msc para expandir capacidade.
                    if self._last_safe_msc_advanced:
                        from_msc = ckp.last_captured_msc + 1
                    else:
                        from_msc = ckp.last_captured_msc
                else:
                    from_msc = max(session_start, ckp.last_captured_msc - lookback_ms)
            else:
                from_msc = session_start

            # Rastreamento de estagnação de cursor
            if from_msc == self._last_cursor_from_msc:
                self._cursor_stagnation_count += 1
                if self._cursor_stagnation_count >= self._stagnation_limit_cycles:
                    logger.warning(
                        f"TELEMETRIA DE ESTAGNAÇÃO: Cursor em {from_msc} estagnado há "
                        f"{self._cursor_stagnation_count} ciclos consecutivos."
                    )
            else:
                self._last_cursor_from_msc = from_msc
                self._cursor_stagnation_count = 0

            # 2. Consultar MT5 com contrato formal
            query_res = None
            if hasattr(self.reader, "query_ticks_from"):
                query_res = self.reader.query_ticks_from(
                    self.symbol, from_msc, count=self._adaptive_batch_size
                )

            if isinstance(query_res, MT5QueryResult):
                if query_res.status == MT5QueryStatus.DISCONNECTED:
                    self._attempt_mt5_reconnect()
                    return 0

                if query_res.status in (MT5QueryStatus.MT5_QUERY_ERROR, MT5QueryStatus.INCONCLUSIVE):
                    logger.warning(
                        f"Consulta MT5 retornou status {query_res.status}: code={query_res.error_code}, "
                        f"msg={query_res.error_message}. Checkpoint mantido sem avanço."
                    )
                    return 0

                if query_res.status == MT5QueryStatus.SUCCESS_EMPTY_VERIFIED:
                    # Intervalo comprovadamente sem novos negócios no broker
                    return 0

                raw_ticks = query_res.ticks
                is_capacity_saturated = query_res.is_capacity_saturated
            else:
                # Fallback para mocks legados ou implementações que expõem apenas fetch_ticks_from
                fetched = self.reader.fetch_ticks_from(
                    self.symbol, from_msc, count=self._adaptive_batch_size
                )
                if isinstance(fetched, list):
                    raw_ticks = fetched
                    is_capacity_saturated = (len(raw_ticks) >= self._adaptive_batch_size)
                else:
                    raw_ticks = []
        else:
            # Em chamadas diretas com ticks pré-definidos (testes / simulações)
            is_capacity_saturated = (len(raw_ticks) >= self._adaptive_batch_size)

        if not raw_ticks:
            return 0

        # 3. Determinar a fronteira de cobertura temporal comprovada (safe_msc)
        max_batch_msc = max(t["time_msc"] for t in raw_ticks)
        ticks_to_persist = []

        if not is_capacity_saturated:
            # Lote NÃO atingiu capacidade: todo o intervalo até max_batch_msc foi entregue
            safe_msc = max_batch_msc
            ticks_to_persist = raw_ticks
            self._adaptive_batch_size = self.config.buffer.batch_size
            self._cursor_stagnation_count = 0
            self._density_stagnation_count = 0
            self._last_safe_msc_advanced = (safe_msc > ckp.last_captured_msc)
        else:
            # Lote atingiu capacidade: risco de truncamento no milissegundo de fronteira!
            sub_mscs = [t["time_msc"] for t in raw_ticks if t["time_msc"] < max_batch_msc]
            if sub_mscs:
                # Lote multi-temporal: a fronteira segura é o maior milissegundo estritamente anterior
                safe_msc = max(sub_mscs)
                # CRÍTICO (H002 B.1.1): Persistir estritamente os ticks comprovadamente completos (time_msc <= safe_msc).
                # O milissegundo final max_batch_msc é retido para ser consultado e reconciliado por inteiro no próximo ciclo.
                ticks_to_persist = [t for t in raw_ticks if t["time_msc"] <= safe_msc]
                self._last_safe_msc_advanced = (safe_msc > ckp.last_captured_msc)
                self._cursor_stagnation_count = 0
                self._density_stagnation_count = 0
            else:
                # Lote mono-temporal ultra-denso (todos os ticks no mesmo milissegundo)
                safe_msc = ckp.last_captured_msc
                ticks_to_persist = []  # NENHUM registro da fronteira não comprovada é persistido!
                self._last_safe_msc_advanced = False

                if len(raw_ticks) < self._max_batch_size:
                    self._adaptive_batch_size = min(self._adaptive_batch_size * 2, self._max_batch_size)
                    self._cursor_stagnation_count += 1
                    logger.info(
                        f"Milissegundo ultra-denso em {max_batch_msc} ({len(raw_ticks)} ticks). "
                        f"Expandindo batch_size para {self._adaptive_batch_size}."
                    )
                else:
                    # Densidade mono-temporal atingiu ou excedeu o limite máximo seguro (ex: 5000)!
                    # Registrar intervalo inconclusivo e acionar proteção fail-closed acionável
                    self._density_stagnation_count += 1
                    inconclusive_event = {
                        "time_msc": max_batch_msc,
                        "tick_count": len(raw_ticks),
                        "max_batch_size": self._max_batch_size,
                        "cursor_from_msc": from_msc,
                        "stagnation_count": self._density_stagnation_count,
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "reason": "MONO_TEMPORAL_DENSITY_EXCEEDED_MAX_BATCH"
                    }
                    self.inconclusive_intervals.append(inconclusive_event)
                    logger.warning(
                        f"INTERVALO INCONCLUSIVO REGISTRADO: Densidade em {max_batch_msc} ({len(raw_ticks)} ticks) "
                        f"excede capacidade máxima ({self._max_batch_size}). Checkpoint travado em {safe_msc}."
                    )

                    if self._density_stagnation_count >= self._stagnation_limit_cycles:
                        self.operational_status = "CRITICAL_STAGNATION_LIMIT_EXCEEDED"
                        self.has_critical_stagnation = True
                        self.last_stagnation_error = (
                            f"Densidade extrema em {max_batch_msc} ({len(raw_ticks)} ticks) "
                            f"excedeu limite seguro ({self._max_batch_size} ticks) por {self._density_stagnation_count} ciclos consecutivos. "
                            f"Fail-closed ativado para impedir perda silenciosa de dados."
                        )
                        logger.critical(f"ESTADO DE ERRO OPERACIONAL ACIONÁVEL: {self.last_stagnation_error}")

        # 4. Reconciliar negócios e filtrar atualizações de book apenas dos registros aprovados
        inserted = 0
        if ticks_to_persist:
            cutoff = self.boot_time_msc if self.boot_time_msc > 0 else None
            reconciled = self.reconciler.reconcile_raw_ticks(
                raw_ticks=ticks_to_persist,
                symbol=self.symbol,
                provenance="REALTIME_STREAM",
                cutoff_realtime_msc=cutoff
            )

            if reconciled:
                # Gravar no SQLite WAL e avançar checkpoint com safe_msc
                inserted = self.spool.append_ticks_batch(
                    reconciled, stream_key=self.symbol, checkpoint_msc=safe_msc
                )
            elif safe_msc > ckp.last_captured_msc:
                # Todos os ticks eram duplicatas já persistidas, mas a cobertura temporal foi comprovada!
                self.spool.update_captured_checkpoint(safe_msc, stream_key=self.symbol)
        elif safe_msc > ckp.last_captured_msc:
            self.spool.update_captured_checkpoint(safe_msc, stream_key=self.symbol)

        self._last_batch_saturated = is_capacity_saturated
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

    def periodic_reconcile_step(self, now_server_msc: Optional[int] = None, session_start_msc: Optional[int] = None) -> Dict[str, Any]:
        """
        Executa uma passada do reconciliador histórico periódico.
        Varre retrospectivamente negócios disponibilizados com atraso (atraso > lookback_ms)
        sem regredir o W_durable (last_captured_msc) e avança W_complete até a cobertura auditada.
        """
        if now_server_msc is None:
            now_utc = datetime.now(timezone.utc)
            now_server_msc = self.time_normalizer.utc_datetime_to_server_msc(now_utc)
            
        if session_start_msc is None:
            ref_dt = self.time_normalizer.server_msc_to_utc_datetime(now_server_msc)
            ref_date = ref_dt.astimezone(self.time_normalizer.market_tz).date()
            session_start_msc = self._get_session_start_msc(ref_date)

        return self.periodic_reconciler.reconcile_cycle(
            now_server_msc=now_server_msc,
            session_start_msc=session_start_msc,
            replicate_fn=self.replicate_pending_to_postgres
        )



    def maybe_periodic_reconcile(self, last_run_timestamp: float, interval_sec: float = 60.0) -> tuple[float, Dict[str, Any]]:
        """
        Aciona a reconciliação periódica se decorrido o intervalo estipulado.
        """
        now = time.time()
        if now - last_run_timestamp >= interval_sec:
            res = self.periodic_reconcile_step()
            return now, res
        return last_run_timestamp, {"status": "SKIPPED_INTERVAL", "recovered_count": 0}

