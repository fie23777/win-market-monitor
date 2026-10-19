"""
Módulo de Normalização Temporal do MetaTrader 5 (B3 / XP).

Responsabilidade exclusiva:
- Distinguir e converter de forma determinística e testável:
  1. Horário real de mercado B3 (America/Sao_Paulo).
  2. Timestamp numérico do servidor MT5 (time, time_msc), preservando seus valores originais.
  3. Horário UTC normalizado para persistência e auditoria global.
- Eliminar offsets arbitrários ou fixos espalhados no código-fonte.
- Garantir consistência integral entre ticks, candles M2 e checkpoints.
"""

from datetime import datetime, timezone, timedelta, date
from typing import Optional, Union
import zoneinfo

class MT5TimeNormalizer:
    """
    Camada de normalização temporal explícita para a integração MT5.
    
    O servidor MetaTrader 5 da XP Investimentos indexa as séries temporais de ticks e candles
    utilizando a hora civil do pregão da B3 (America/Sao_Paulo) diretamente em seus timestamps numéricos
    (time e time_msc).
    
    Esta classe traduz janelas de tempo entre o referencial do servidor MT5 e UTC real,
    sem jamais alterar os inteiros time e time_msc originais gerados pelo terminal.
    """

    def __init__(
        self,
        server_timezone: str = "America/Sao_Paulo",
        market_timezone: str = "America/Sao_Paulo"
    ):
        self.server_tz = zoneinfo.ZoneInfo(server_timezone)
        self.market_tz = zoneinfo.ZoneInfo(market_timezone)

    def server_msc_to_utc_datetime(self, server_time_msc: int) -> datetime:
        """
        Converte o timestamp numérico bruto retornado pelo MT5 (time_msc)
        para o datetime UTC real do evento na B3.
        
        Exemplo:
        server_time_msc = 1792400400123 (09:00:00.123 no relógio do servidor XP)
        -> 2026-10-19 12:00:00.123000+00:00 UTC
        """
        seconds = server_time_msc // 1000
        ms = server_time_msc % 1000
        # Interpretar seconds como horário civil no fuso do servidor
        naive_dt = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(tzinfo=None)
        server_dt = naive_dt.replace(tzinfo=self.server_tz)
        utc_dt = server_dt.astimezone(timezone.utc) + timedelta(milliseconds=ms)
        return utc_dt

    def server_seconds_to_utc_datetime(self, server_seconds: int) -> datetime:
        """
        Converte timestamp em segundos inteiros (ex: candle time) para datetime UTC real.
        """
        naive_dt = datetime.fromtimestamp(server_seconds, tz=timezone.utc).replace(tzinfo=None)
        server_dt = naive_dt.replace(tzinfo=self.server_tz)
        return server_dt.astimezone(timezone.utc)

    def utc_datetime_to_server_msc(self, dt_utc: datetime) -> int:
        """
        Converte um datetime UTC real para o timestamp numérico time_msc
        correspondente no relógio do servidor MT5.
        
        Exemplo:
        2026-10-19 12:00:00.123000+00:00 UTC
        -> 1792400400123
        """
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_utc.astimezone(timezone.utc)
            
        server_dt = dt_utc.astimezone(self.server_tz)
        naive_server = server_dt.replace(tzinfo=None)
        server_sec = int(naive_server.replace(tzinfo=timezone.utc).timestamp())
        ms = dt_utc.microsecond // 1000
        return server_sec * 1000 + ms

    def market_datetime_to_server_datetime(self, dt_market: datetime) -> datetime:
        """
        Converte datetime no fuso de mercado para o datetime exato
        esperado pelas funções copy_ticks_range / copy_rates_range do MT5.
        """
        if dt_market.tzinfo is None:
            dt_market = dt_market.replace(tzinfo=self.market_tz)
        else:
            dt_market = dt_market.astimezone(self.market_tz)
            
        dt_server = dt_market.astimezone(self.server_tz)
        naive_server = dt_server.replace(tzinfo=None)
        # Retorna datetime com tzinfo=timezone.utc para garantir que a extensão C do MT5
        # extraia o epoch nominal desejado sem interferência da timezone local do Windows.
        return naive_server.replace(tzinfo=timezone.utc)

    def utc_datetime_to_server_datetime(self, dt_utc: datetime) -> datetime:
        """
        Converte datetime UTC real para o datetime exato esperado pelas funções do MT5.
        """
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt_utc.astimezone(timezone.utc)
            
        dt_server = dt_utc.astimezone(self.server_tz)
        naive_server = dt_server.replace(tzinfo=None)
        return naive_server.replace(tzinfo=timezone.utc)

    def get_session_start_server_msc(
        self,
        target_date: Optional[date] = None,
        hour: int = 9,
        minute: int = 0
    ) -> int:
        """
        Retorna o time_msc no referencial do servidor MT5 para a abertura da sessão
        (padrão B3: 09:00 BRT).
        Para 19/10/2026 às 09:00:00 -> 1792400400000 msc.
        """
        if target_date is None:
            target_date = datetime.now(timezone.utc).astimezone(self.market_tz).date()
        market_session = datetime(
            target_date.year, target_date.month, target_date.day,
            hour, minute, 0, tzinfo=self.market_tz
        )
        dt_server = market_session.astimezone(self.server_tz)
        naive_server = dt_server.replace(tzinfo=None)
        server_sec = int(naive_server.replace(tzinfo=timezone.utc).timestamp())
        return server_sec * 1000

    def server_msc_to_market_datetime(self, server_time_msc: int) -> datetime:
        """
        Converte server_time_msc para a representação civil no fuso de mercado
        (America/Sao_Paulo) para fins de logs, auditoria e exibição humana.
        """
        utc_dt = self.server_msc_to_utc_datetime(server_time_msc)
        return utc_dt.astimezone(self.market_tz)
