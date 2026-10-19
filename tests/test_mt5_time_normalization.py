"""
Suíte de Testes da Normalização Temporal do MetaTrader 5 (B3 / XP).

Validações obrigatórias:
1. Conversão temporal bidirecional e simétrica (UTC <-> MT5 Server Time <-> Mercado BRT).
2. Preservação rigorosa e imutável dos valores inteiros originais de time e time_msc.
3. Compatibilidade e resolução determinística das queries no MT5 (copy_ticks_range, copy_rates_range).
4. Reconciliação com timestamps BRT normalizados para UTC no TradeTick e M2CandleClosed.
5. Filtro estrito de negócios e tratamento de flags combinadas.
6. Limites temporais e integridade dos candles M2.
7. Checkpoints de dois estágios sem dupla conversão após reinicializações.
8. Deduplicação determinística em consultas sobrepostas.
9. Proveniência RECOVERED_HISTORICAL_MT5 para dados históricos anteriores ao boot.
10. Isolamento estrito do banco de testes contra o banco operacional.
11. Auditoria zero de métodos ou pacotes de trading/ordens (D015 intacto).
"""

import pytest
import zoneinfo
from datetime import datetime, timezone, timedelta, date
from unittest.mock import MagicMock, patch

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.connection.mt5_reader import MT5ReadOnlyReader
from src.reconciliation.tick_reconciler import TickReconciler
from src.domain.models import TradeTick, M2CandleClosed
from src.buffer.durable_spool import DurableSpool
from src.config import AppConfig, BufferConfig, PostgresConfig, ParquetConfig, CollectorSettings
from src.engine import MarketCollectorEngine

BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")

@pytest.fixture
def normalizer():
    return MT5TimeNormalizer(server_timezone="America/Sao_Paulo", market_timezone="America/Sao_Paulo")

def test_bidirectional_time_conversion_symmetry(normalizer):
    """Garante conversão simétrica e determinística entre MT5 Server msc e UTC."""
    # 19/10/2026 às 09:00:00.123 BRT
    # Server Time da XP indexa 09:00:00 civil
    server_msc = 1792400400123
    
    # 1. Server msc -> UTC
    utc_dt = normalizer.server_msc_to_utc_datetime(server_msc)
    assert utc_dt.tzinfo == timezone.utc
    assert utc_dt.year == 2026 and utc_dt.month == 10 and utc_dt.day == 19
    assert utc_dt.hour == 12 and utc_dt.minute == 0 and utc_dt.second == 0
    assert utc_dt.microsecond == 123000
    
    # 2. UTC -> Server msc (inversa exata)
    recovered_msc = normalizer.utc_datetime_to_server_msc(utc_dt)
    assert recovered_msc == server_msc
    
    # 3. Server msc -> Market civil
    market_dt = normalizer.server_msc_to_market_datetime(server_msc)
    assert market_dt.tzinfo == BRT_TZ
    assert market_dt.hour == 9 and market_dt.minute == 0 and market_dt.second == 0
    assert market_dt.microsecond == 123000

def test_session_start_calculation(normalizer):
    """Verifica que 09:00 BRT resulta exatamente no time_msc do servidor correspondente."""
    target_date = date(2026, 10, 19)
    session_msc = normalizer.get_session_start_server_msc(target_date, hour=9, minute=0)
    assert session_msc == 1792400400000
    
    utc_dt = normalizer.server_msc_to_utc_datetime(session_msc)
    assert utc_dt == datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)

def test_query_datetime_resolution_in_reader(normalizer):
    """Verifica como MT5ReadOnlyReader traduz datetimes e timestamps para chamadas MT5."""
    reader = MT5ReadOnlyReader(time_normalizer=normalizer)
    
    # Datetime UTC aware (12:00:00 UTC) -> deve virar Server Time (09:00:00 UTC para a C-extension)
    dt_utc = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
    res_dt = reader._resolve_query_datetime(dt_utc)
    assert res_dt == datetime(2026, 10, 19, 9, 0, 0, tzinfo=timezone.utc)
    assert int(res_dt.timestamp()) == 1792400400
    
    # Timestamp numérico em milissegundos
    res_msc = reader._resolve_query_datetime(1792400400000)
    assert int(res_msc.timestamp()) == 1792400400
    
    # Timestamp numérico em segundos inteiros
    res_sec = reader._resolve_query_datetime(1792400400)
    assert int(res_sec.timestamp()) == 1792400400

def test_tick_reconciler_preserves_time_msc_and_normalizes_utc(normalizer):
    """Garante que o TickReconciler preserva time_msc bruto e normaliza time_utc."""
    reconciler = TickReconciler(time_normalizer=normalizer)
    
    server_msc = 1792400400500 # 09:00:00.500 BRT
    raw_ticks = [{
        "time": server_msc // 1000,
        "time_msc": server_msc,
        "last": 210500.0,
        "volume_real": 10.0,
        "flags": 32 # BUY
    }]
    
    reconciled = reconciler.reconcile_raw_ticks(raw_ticks, symbol="WINZ26")
    assert len(reconciled) == 1
    tt = reconciled[0]
    
    # Preservação estrita do time_msc original
    assert tt.time_msc == server_msc
    # Normalização de time_utc para UTC real (12:00:00.500)
    assert tt.time_utc == datetime(2026, 10, 19, 12, 0, 0, 500000, tzinfo=timezone.utc)
    assert tt.price == 210500.0
    assert tt.volume_real == 10.0
    assert tt.trade_side == "BUY"

def test_combined_flags_filtering(normalizer):
    """Valida filtro de negócios com flags combinadas (rejeição de book puro, aceitação de trade)."""
    reconciler = TickReconciler(time_normalizer=normalizer)
    server_msc = 1792400401000
    
    raw = [
        # Atualização exclusiva de book (BID|ASK = 6) -> Rejeitar
        {"time": server_msc // 1000, "time_msc": server_msc, "last": 210000.0, "volume_real": 0.0, "flags": 6},
        # Negócio válido com LAST + VOLUME + BUY (flags = 8 | 16 | 32 = 56) -> Aceitar
        {"time": server_msc // 1000, "time_msc": server_msc + 1, "last": 210005.0, "volume_real": 2.0, "flags": 56},
        # Negócio válido com SELL + LAST (flags = 64 | 8 = 72) -> Aceitar
        {"time": server_msc // 1000, "time_msc": server_msc + 2, "last": 210000.0, "volume_real": 1.0, "flags": 72},
    ]
    
    reconciled = reconciler.reconcile_raw_ticks(raw, symbol="WINZ26")
    assert len(reconciled) == 2
    assert reconciled[0].trade_side == "BUY"
    assert reconciled[1].trade_side == "SELL"

def test_m2_candle_temporal_boundaries(normalizer):
    """Valida limites temporais de candles M2 gerados a partir do MT5."""
    server_candle_time = 1792400400 # 09:00:00 BRT
    open_utc = normalizer.server_seconds_to_utc_datetime(server_candle_time)
    close_utc = open_utc + timedelta(minutes=2)
    
    candle = M2CandleClosed(
        symbol="WINZ26",
        open_time_utc=open_utc,
        close_time_utc=close_utc,
        open=210000.0,
        high=210100.0,
        low=209950.0,
        close=210050.0,
        volume_real=1500.0,
        tick_volume=800,
        spread=5
    )
    candle.validate()
    
    assert candle.open_time_utc == datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
    assert candle.close_time_utc == datetime(2026, 10, 19, 12, 2, 0, tzinfo=timezone.utc)

def test_checkpoints_persistence_after_restart(temp_spool_dir, normalizer):
    """Verifica que o checkpoint armazena o time_msc original e não sofre desvio após reinício."""
    db_file = temp_spool_dir / "ckp_test.db"
    spool1 = DurableSpool(db_file)
    
    server_msc = 1792400500123
    tt = TradeTick(
        symbol="WINZ26",
        time_msc=server_msc,
        time_utc=normalizer.server_msc_to_utc_datetime(server_msc),
        price=210500.0,
        volume_real=5.0,
        flags=32,
        trade_side="BUY"
    )
    spool1.append_ticks_batch([tt], stream_key="WINZ26")
    
    ckp1 = spool1.get_checkpoints("WINZ26")
    assert ckp1.last_captured_msc == server_msc
    
    # Simular reinício do processo instanciando novo DurableSpool no mesmo arquivo
    spool2 = DurableSpool(db_file)
    ckp2 = spool2.get_checkpoints("WINZ26")
    assert ckp2.last_captured_msc == server_msc
    assert ckp2.last_replicated_msc == 0

def test_overlapping_query_deduplication(normalizer):
    """Verifica que reconsultas com sobreposição não duplicam ticks já emitidos."""
    reconciler = TickReconciler(time_normalizer=normalizer)
    server_msc = 1792400400000
    
    batch1 = [
        {"time": server_msc // 1000, "time_msc": server_msc, "last": 210000.0, "volume_real": 1.0, "flags": 32},
        {"time": server_msc // 1000, "time_msc": server_msc + 10, "last": 210005.0, "volume_real": 2.0, "flags": 32},
    ]
    r1 = reconciler.reconcile_raw_ticks(batch1, symbol="WINZ26")
    assert len(r1) == 2
    
    # Lote 2 sobreposto contendo os mesmos ticks anteriores e um novo
    batch2 = [
        {"time": server_msc // 1000, "time_msc": server_msc, "last": 210000.0, "volume_real": 1.0, "flags": 32},
        {"time": server_msc // 1000, "time_msc": server_msc + 10, "last": 210005.0, "volume_real": 2.0, "flags": 32},
        {"time": server_msc // 1000, "time_msc": server_msc + 20, "last": 210010.0, "volume_real": 3.0, "flags": 32},
    ]
    r2 = reconciler.reconcile_raw_ticks(batch2, symbol="WINZ26")
    assert len(r2) == 1
    assert r2[0].time_msc == server_msc + 20

def test_historical_provenance_classification(normalizer):
    """Garante que ticks anteriores ao boot_time_msc recebam proveniência RECOVERED_HISTORICAL_MT5."""
    reconciler = TickReconciler(time_normalizer=normalizer)
    
    boot_server_msc = 1792401600000 # ~09:20 BRT
    raw = [
        # Tick anterior ao boot -> Histórico recuperado
        {"time": 1792400500, "time_msc": 1792400500000, "last": 210000.0, "volume_real": 1.0, "flags": 32},
        # Tick posterior ao boot -> Tempo real
        {"time": 1792401605, "time_msc": 1792401605000, "last": 210010.0, "volume_real": 2.0, "flags": 32},
    ]
    
    reconciled = reconciler.reconcile_raw_ticks(
        raw_ticks=raw,
        symbol="WINZ26",
        provenance="REALTIME_STREAM",
        cutoff_realtime_msc=boot_server_msc
    )
    
    assert len(reconciled) == 2
    assert reconciled[0].provenance == "RECOVERED_HISTORICAL_MT5"
    assert reconciled[1].provenance == "REALTIME_STREAM"
