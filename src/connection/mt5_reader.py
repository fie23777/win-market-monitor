import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

from src.connection.mt5_time_normalizer import MT5TimeNormalizer

logger = logging.getLogger(__name__)

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
        Converte datetime UTC/Mercado ou timestamp inteiro para o datetime
        esperado pelas chamadas copy_ticks_* / copy_rates_* da C-extension do MT5.
        """
        if isinstance(dt_or_msc, (int, float)):
            # Se for milissegundos (> 10^11), converter para segundos inteiros
            sec = int(dt_or_msc // 1000) if dt_or_msc > 1e11 else int(dt_or_msc)
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

    def fetch_ticks_range(
        self,
        symbol: str,
        from_time: Any,
        to_time: Any
    ) -> List[Dict[str, Any]]:
        """
        Consulta histórico de ticks no MT5 no intervalo especificado.
        Aceita datetime (UTC ou mercado) ou timestamps numéricos.
        Preserva rigorosamente os inteiros time e time_msc originais.
        """
        if not mt5:
            return []

        dt_from = self._resolve_query_datetime(from_time)
        dt_to = self._resolve_query_datetime(to_time)

        ticks_arr = mt5.copy_ticks_range(symbol, dt_from, dt_to, mt5.COPY_TICKS_ALL)
        if ticks_arr is None or len(ticks_arr) == 0:
            return []

        ticks_list = []
        for t in ticks_arr:
            ticks_list.append({
                "time": int(t["time"]),
                "time_msc": int(t["time_msc"]),
                "bid": float(t["bid"]),
                "ask": float(t["ask"]),
                "last": float(t["last"]),
                "volume": float(t["volume"]),
                "volume_real": float(t["volume_real"]),
                "flags": int(t["flags"]),
            })
        return ticks_list

    def fetch_ticks_from(
        self,
        symbol: str,
        from_time: Any,
        count: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Consulta os próximos ticks a partir de um instante inicial com limite de contagem.
        Aceita datetime (UTC ou mercado) ou timestamp numérico msc.
        Preserva rigorosamente os inteiros time e time_msc originais.
        """
        if not mt5:
            return []

        dt_from = self._resolve_query_datetime(from_time)

        ticks_arr = mt5.copy_ticks_from(symbol, dt_from, count, mt5.COPY_TICKS_ALL)
        if ticks_arr is None or len(ticks_arr) == 0:
            return []

        ticks_list = []
        for t in ticks_arr:
            ticks_list.append({
                "time": int(t["time"]),
                "time_msc": int(t["time_msc"]),
                "bid": float(t["bid"]),
                "ask": float(t["ask"]),
                "last": float(t["last"]),
                "volume": float(t["volume"]),
                "volume_real": float(t["volume_real"]),
                "flags": int(t["flags"]),
            })
        return ticks_list

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
