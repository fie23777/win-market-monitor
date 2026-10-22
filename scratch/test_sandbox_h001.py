import pytest
import sqlite3
import tempfile
import gc
import os
from unittest.mock import MagicMock
from datetime import datetime, timezone, timedelta

from scratch.sandbox_solution import SafeRangePaginator, DecoupledHistoricalReconciler, PaginationStallError
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.buffer.durable_spool import DurableSpool
from src.domain.models import TradeTick, Checkpoints

def create_synthetic_ticks(count: int, start_msc: int = 1792573200000, msc_step: int = 1, density_same_msc: int = 1):
    ticks = []
    current_msc = start_msc
    for i in range(count):
        if i > 0 and i % density_same_msc == 0:
            current_msc += msc_step
        ticks.append({
            "time": int(current_msc // 1000),
            "time_msc": current_msc,
            "bid": 212500.0,
            "ask": 212505.0,
            "last": 212500.0,
            "volume": 10.0,
            "volume_real": 10.0,
            "flags": 1080 # DEAL_BUY
        })
    return ticks

def test_safe_paginator_handles_burst_over_1000():
    """Testa se o SafeRangePaginator fatia e captura rajadas de mais de 1.000 ticks."""
    normalizer = MT5TimeNormalizer()
    mock_reader = MagicMock()
    
    # 5.000 ticks em 2 segundos
    synthetic = create_synthetic_ticks(count=5000, start_msc=1792573258000, msc_step=1, density_same_msc=5)
    mock_reader.fetch_ticks_range.return_value = synthetic

    paginator = SafeRangePaginator(reader=mock_reader, time_normalizer=normalizer, max_slice_ms=10_000)
    ticks, slice_end, is_complete = paginator.fetch_safe_slice(
        symbol="WINZ26",
        from_msc=1792573258000,
        target_ceiling_msc=1792573268000,
        lookback_ms=0
    )
    assert len(ticks) == 5000
    assert is_complete is True
    mock_reader.fetch_ticks_range.assert_called_once()

def test_safe_paginator_handles_massive_burst_50000_and_100000():
    """Testa suporte a rajadas maciças de 50.000 e 100.000 registros."""
    normalizer = MT5TimeNormalizer()
    mock_reader = MagicMock()
    
    synthetic_50k = create_synthetic_ticks(count=50000, start_msc=1792573200000, msc_step=1, density_same_msc=10)
    mock_reader.fetch_ticks_range.return_value = synthetic_50k

    paginator = SafeRangePaginator(reader=mock_reader, time_normalizer=normalizer, max_slice_ms=60_000)
    ticks, slice_end, is_complete = paginator.fetch_safe_slice(
        symbol="WINZ26",
        from_msc=1792573200000,
        target_ceiling_msc=1792573260000,
        lookback_ms=0
    )
    assert len(ticks) == 50000

    synthetic_100k = create_synthetic_ticks(count=100000, start_msc=1792573200000, msc_step=1, density_same_msc=20)
    mock_reader.fetch_ticks_range.return_value = synthetic_100k
    ticks_100k, _, _ = paginator.fetch_safe_slice(
        symbol="WINZ26",
        from_msc=1792573200000,
        target_ceiling_msc=1792573260000,
        lookback_ms=0
    )
    assert len(ticks_100k) == 100000

def test_multi_trades_in_same_millisecond_preserved():
    """Garante que múltiplos negócios no mesmo milissegundo são preservados com ocorrência ordinal distinta."""
    from src.reconciliation.tick_reconciler import TickReconciler
    normalizer = MT5TimeNormalizer()
    reconciler = TickReconciler(time_normalizer=normalizer)

    # 10 trades no exato mesmo milissegundo 1792573258338
    raw = [
        {
            "time": 1792573258,
            "time_msc": 1792573258338,
            "bid": 212900.0,
            "ask": 212905.0,
            "last": 212900.0,
            "volume": 100.0,
            "volume_real": 100.0,
            "flags": 1080
        }
        for _ in range(10)
    ]
    reconciled = reconciler.reconcile_raw_ticks(raw, "WINZ26", "TEST")
    assert len(reconciled) == 10
    ordinals = [t.occurrence_ordinal for t in reconciled]
    assert ordinals == list(range(1, 11))
    fingerprints = [t.tick_fingerprint for t in reconciled]
    assert len(set(fingerprints)) == 10 # Cada um tem fingerprint único com ordinal!

def test_decoupled_historical_reconciler_advances_when_streaming_stalled():
    """
    TESTE CRÍTICO DO BUG DO INCIDENTE:
    Comprova que quando last_captured_msc está paralisado em 09:00:58,
    o DecoupledHistoricalReconciler NÃO fica preso em UP_TO_DATE e avança retrospectivamente!
    """
    normalizer = MT5TimeNormalizer()
    db_path = "scratch/test_sandbox_spool.db"
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except Exception:
            pass

    try:
        spool = DurableSpool(db_path=db_path, time_normalizer=normalizer)
        
        # Simular estado congelado do incidente:
        stuck_msc = 1792573258338 # 09:00:58.338
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
            INSERT INTO checkpoints_two_stage (stream_key, last_captured_msc, last_replicated_msc, watermark_complete_msc, updated_at_utc)
            VALUES ('WINZ26', ?, ?, ?, '2026-10-21T09:01:28+00:00');
            """, (stuck_msc, stuck_msc, stuck_msc))

        mock_reader = MagicMock()
        mock_reader.is_connected.return_value = True
        
        # Agora são 09:05:00 BRT (1792573500000 msc)
        now_server_msc = 1792573500000
        
        # O MT5 histórico retorna 500 novos ticks entre 09:00:58 e 09:05:00
        historical_ticks = create_synthetic_ticks(500, start_msc=stuck_msc + 1, msc_step=10)
        mock_reader.fetch_ticks_range.return_value = historical_ticks

        reconciler = DecoupledHistoricalReconciler(
            reader=mock_reader,
            spool=spool,
            symbol="WINZ26",
            safety_lag_ms=2000,
            time_normalizer=normalizer
        )

        res = reconciler.reconcile_cycle(now_server_msc=now_server_msc)

        # O reconciliador NÃO retornou UP_TO_DATE: ele auditou a janela e avançou W_complete!
        assert res["status"] == "RECONCILED"
        assert res["recovered_count"] == 500
        
        ckp_after = spool.get_checkpoints("WINZ26")
        assert ckp_after.watermark_complete_msc > stuck_msc
        print(f"Sucesso: W_complete avançou de {stuck_msc} para {ckp_after.watermark_complete_msc}!")
    finally:
        del spool
        gc.collect()
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
            except Exception:
                pass
