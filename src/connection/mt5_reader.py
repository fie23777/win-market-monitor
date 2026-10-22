import time
import logging
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

from src.connection.mt5_time_normalizer import MT5TimeNormalizer

logger = logging.getLogger(__name__)

class MT5QueryStatus(str, Enum):
    """Contrato estrito de resultado de consulta MT5."""
    SUCCESS_WITH_DATA = "SUCCESS_WITH_DATA"
    SUCCESS_EMPTY_VERIFIED = "SUCCESS_EMPTY_VERIFIED"
    MT5_QUERY_ERROR = "MT5_QUERY_ERROR"
    DISCONNECTED = "DISCONNECTED"
    INCONCLUSIVE = "INCONCLUSIVE"

@dataclass
class MT5QueryResult:
    """
    Encapsula o resultado bruto de uma consulta ao MetaTrader 5.
    ATENÇÃO: SUCCESS_WITH_DATA comprova apenas sucesso da chamada C-API,
    NÃO comprova por si só completude integral de uma janela temporal densa.
    """
    status: MT5QueryStatus
    ticks: List[Dict[str, Any]] = field(default_factory=list)
    requested_from: Any = None
    requested_to_or_count: Any = None
    error_code: int = 0
    error_message: str = ""
    query_duration_ms: float = 0.0
    is_capacity_saturated: bool = False

class MT5ReadOnlyReader:
    """
    Leitor estrito de mercado via MetaTrader 5 (Modo Somente Leitura).
    PROIBIDO: Qualquer implementação ou importação de envio ou modificação de ordens.
    """

    def __init__(self, time_normalizer: Optional[MT5TimeNormalizer] = None):
        self.connected = False
        self.time_normalizer = time_normalizer or MT5TimeNormalizer()

    def initialize(self, path: Optional[str] = None) -> bool:
        """Inicializa a conexão com o terminal MetaTrader 5."""
        if mt5 is None:
            logger.warning("Biblioteca MetaTrader5 não instalada neste ambiente.")
            return False

        init_kwargs = {}
        if path:
            init_kwargs["path"] = path

        if not mt5.initialize(**init_kwargs):
            err = mt5.last_error()
            logger.error(f"Falha ao inicializar MT5: {err}")
            self.connected = False
            return False

        t_info = mt5.terminal_info()
        self.connected = bool(t_info and t_info.connected)
        logger.info(f"MT5 Inicializado. Terminal conectado ao broker: {self.connected}")
        return True

    def shutdown(self):
        """Encerra a conexão com o MT5."""
        if mt5 and self.connected:
            mt5.shutdown()
            self.connected = False

    def is_connected(self) -> bool:
        """Verifica se o terminal está conectado ao servidor da corretora."""
        if not mt5:
            return False
        t_info = mt5.terminal_info()
        return bool(t_info and t_info.connected)

    def select_symbol(self, symbol: str) -> bool:
        """Garante que o símbolo esteja ativo no Market Watch."""
        if not mt5:
            return False
        return bool(mt5.symbol_select(symbol, True))

    def _resolve_query_datetime(self, dt_or_msc: Any) -> datetime:
        """
        Converte datetime UTC/Mercado ou timestamp numérico para o datetime
        esperado pelas chamadas copy_ticks_* / copy_rates_* da C-extension do MT5.
        PRESERVA frações de segundos (milissegundos / microssegundos).
        """
        if isinstance(dt_or_msc, (int, float)):
            # Se for milissegundos (> 10^11), converter dividindo por 1000.0 (float)
            sec = dt_or_msc / 1000.0 if dt_or_msc > 1e11 else float(dt_or_msc)
            return datetime.fromtimestamp(sec, tz=timezone.utc)
        elif isinstance(dt_or_msc, datetime):
            if dt_or_msc.tzinfo is not None:
                # Normalizar datetime ciente de timezone para o Server Time do MT5
                return self.time_normalizer.utc_datetime_to_server_datetime(dt_or_msc)
            else:
                # Naive: tratar como horário já no referencial do servidor
                return dt_or_msc.replace(tzinfo=timezone.utc)
        else:
            raise ValueError(f"Tipo temporal inválido para consulta MT5: {type(dt_or_msc)}")

    def query_ticks_range(
        self,
        symbol: str,
        from_time: Any,
        to_time: Any
    ) -> MT5QueryResult:
        """
        Executa consulta histórica no MT5 por intervalo com contrato explícito de status.
        Diferencia vazio verificado, erro e desconexão.
        """
        t0 = time.perf_counter()
        if not mt5 or not self.is_connected():
            dur_ms = (time.perf_counter() - t0) * 1000.0
            return MT5QueryResult(
                status=MT5QueryStatus.DISCONNECTED,
                ticks=[],
                requested_from=from_time,
                requested_to_or_count=to_time,
                error_message="Terminal MT5 não inicializado ou desconectado",
                query_duration_ms=dur_ms
            )

        dt_from = self._resolve_query_datetime(from_time)
        dt_to = self._resolve_query_datetime(to_time)

        try:
            ticks_arr = mt5.copy_ticks_range(symbol, dt_from, dt_to, mt5.COPY_TICKS_ALL)
        except Exception as e:
            dur_ms = (time.perf_counter() - t0) * 1000.0
            return MT5QueryResult(
                status=MT5QueryStatus.MT5_QUERY_ERROR,
                ticks=[],
                requested_from=from_time,
                requested_to_or_count=to_time,
                error_message=f"Exceção ao chamar copy_ticks_range: {e}",
                query_duration_ms=dur_ms
            )

        dur_ms = (time.perf_counter() - t0) * 1000.0

        if ticks_arr is None:
            err = mt5.last_error() if hasattr(mt5, "last_error") else (0, "")
            err_code, err_msg = err if isinstance(err, tuple) else (int(err), str(err))
            if err_code != 0:
                return MT5QueryResult(
                    status=MT5QueryStatus.MT5_QUERY_ERROR,
                    ticks=[],
                    requested_from=from_time,
                    requested_to_or_count=to_time,
                    error_code=err_code,
                    error_message=str(err_msg),
                    query_duration_ms=dur_ms
                )
            return MT5QueryResult(
                status=MT5QueryStatus.INCONCLUSIVE,
                ticks=[],
                requested_from=from_time,
                requested_to_or_count=to_time,
                error_message="MT5 copy_ticks_range retornou None sem código de erro explícito",
                query_duration_ms=dur_ms
            )

        if len(ticks_arr) == 0:
            err = mt5.last_error() if hasattr(mt5, "last_error") else (0, "")
            err_code = err[0] if isinstance(err, tuple) else 0
            if err_code == 0 and self.is_connected():
                return MT5QueryResult(
                    status=MT5QueryStatus.SUCCESS_EMPTY_VERIFIED,
                    ticks=[],
                    requested_from=from_time,
                    requested_to_or_count=to_time,
                    query_duration_ms=dur_ms
                )
            return MT5QueryResult(
                status=MT5QueryStatus.INCONCLUSIVE,
                ticks=[],
                requested_from=from_time,
                requested_to_or_count=to_time,
                error_code=err_code,
                error_message="Lista vazia retornada sob estado indeterminado ou erro do MT5",
                query_duration_ms=dur_ms
            )

        ticks_list = [
            {
                "time": int(t["time"]),
                "time_msc": int(t["time_msc"]),
                "bid": float(t["bid"]),
                "ask": float(t["ask"]),
                "last": float(t["last"]),
                "volume": float(t["volume"]),
                "volume_real": float(t["volume_real"]),
                "flags": int(t["flags"]),
            }
            for t in ticks_arr
        ]

        return MT5QueryResult(
            status=MT5QueryStatus.SUCCESS_WITH_DATA,
            ticks=ticks_list,
            requested_from=from_time,
            requested_to_or_count=to_time,
            query_duration_ms=dur_ms,
            is_capacity_saturated=False
        )

    def query_ticks_from(
        self,
        symbol: str,
        from_time: Any,
        count: int = 1000
    ) -> MT5QueryResult:
        """
        Executa consulta no MT5 a partir de um instante com contagem e contrato explícito de status.
        Identifica se a capacidade foi saturada (len(ticks) >= count).
        """
        t0 = time.perf_counter()
        if not mt5 or not self.is_connected():
            dur_ms = (time.perf_counter() - t0) * 1000.0
            return MT5QueryResult(
                status=MT5QueryStatus.DISCONNECTED,
                ticks=[],
                requested_from=from_time,
                requested_to_or_count=count,
                error_message="Terminal MT5 não inicializado ou desconectado",
                query_duration_ms=dur_ms
            )

        dt_from = self._resolve_query_datetime(from_time)

        try:
            ticks_arr = mt5.copy_ticks_from(symbol, dt_from, count, mt5.COPY_TICKS_ALL)
        except Exception as e:
            dur_ms = (time.perf_counter() - t0) * 1000.0
            return MT5QueryResult(
                status=MT5QueryStatus.MT5_QUERY_ERROR,
                ticks=[],
                requested_from=from_time,
                requested_to_or_count=count,
                error_message=f"Exceção ao chamar copy_ticks_from: {e}",
                query_duration_ms=dur_ms
            )

        dur_ms = (time.perf_counter() - t0) * 1000.0

        if ticks_arr is None:
            err = mt5.last_error() if hasattr(mt5, "last_error") else (0, "")
            err_code, err_msg = err if isinstance(err, tuple) else (int(err), str(err))
            if err_code != 0:
                return MT5QueryResult(
                    status=MT5QueryStatus.MT5_QUERY_ERROR,
                    ticks=[],
                    requested_from=from_time,
                    requested_to_or_count=count,
                    error_code=err_code,
                    error_message=str(err_msg),
                    query_duration_ms=dur_ms
                )
            return MT5QueryResult(
                status=MT5QueryStatus.INCONCLUSIVE,
                ticks=[],
                requested_from=from_time,
                requested_to_or_count=count,
                error_message="MT5 copy_ticks_from retornou None sem código de erro explícito",
                query_duration_ms=dur_ms
            )

        if len(ticks_arr) == 0:
            err = mt5.last_error() if hasattr(mt5, "last_error") else (0, "")
            err_code = err[0] if isinstance(err, tuple) else 0
            if err_code == 0 and self.is_connected():
                return MT5QueryResult(
                    status=MT5QueryStatus.SUCCESS_EMPTY_VERIFIED,
                    ticks=[],
                    requested_from=from_time,
                    requested_to_or_count=count,
                    query_duration_ms=dur_ms
                )
            return MT5QueryResult(
                status=MT5QueryStatus.INCONCLUSIVE,
                ticks=[],
                requested_from=from_time,
                requested_to_or_count=count,
                error_code=err_code,
                error_message="Lista vazia retornada sob estado indeterminado ou erro do MT5",
                query_duration_ms=dur_ms
            )

        ticks_list = [
            {
                "time": int(t["time"]),
                "time_msc": int(t["time_msc"]),
                "bid": float(t["bid"]),
                "ask": float(t["ask"]),
                "last": float(t["last"]),
                "volume": float(t["volume"]),
                "volume_real": float(t["volume_real"]),
                "flags": int(t["flags"]),
            }
            for t in ticks_arr
        ]

        is_saturated = bool(len(ticks_arr) >= count)

        return MT5QueryResult(
            status=MT5QueryStatus.SUCCESS_WITH_DATA,
            ticks=ticks_list,
            requested_from=from_time,
            requested_to_or_count=count,
            query_duration_ms=dur_ms,
            is_capacity_saturated=is_saturated
        )

    def fetch_ticks_range(
        self,
        symbol: str,
        from_time: Any,
        to_time: Any
    ) -> List[Dict[str, Any]]:
        """
        Consulta histórico de ticks no MT5 no intervalo especificado.
        Compatibilidade legada: delega para query_ticks_range e retorna ticks se sucesso.
        """
        res = self.query_ticks_range(symbol, from_time, to_time)
        return res.ticks

    def fetch_ticks_from(
        self,
        symbol: str,
        from_time: Any,
        count: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Consulta os próximos ticks a partir de um instante inicial com limite de contagem.
        Compatibilidade legada: delega para query_ticks_from e retorna ticks se sucesso.
        """
        res = self.query_ticks_from(symbol, from_time, count)
        return res.ticks

    def fetch_rates_range(
        self,
        symbol: str,
        timeframe: int,
        from_time: Any,
        to_time: Any
    ) -> List[Dict[str, Any]]:
        """
        Consulta candles históricos do MT5 no intervalo especificado.
        """
        if not mt5:
            return []

        dt_from = self._resolve_query_datetime(from_time)
        dt_to = self._resolve_query_datetime(to_time)

        rates_arr = mt5.copy_rates_range(symbol, timeframe, dt_from, dt_to)
        if rates_arr is None or len(rates_arr) == 0:
            return []

        rates_list = []
        for r in rates_arr:
            rates_list.append({
                "time": int(r["time"]),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "tick_volume": int(r["tick_volume"]),
                "spread": int(r["spread"]),
                "real_volume": float(r["real_volume"]),
            })
        return rates_list
