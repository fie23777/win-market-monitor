import pytest
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from src.domain.models import TradeTick
from src.buffer.durable_spool import DurableSpool

@pytest.fixture
def temp_spool_dir(tmp_path):
    return tmp_path / "buffer"

@pytest.fixture
def durable_spool(temp_spool_dir):
    db_file = temp_spool_dir / "test_spool.db"
    return DurableSpool(db_file)

@pytest.fixture
def sample_raw_ticks_same_ms():
    """Gera 3 negócios no mesmo milissegundo com campos perfeitamente idênticos."""
    t_msc = 1791881700150
    return [
        {"time": 1791881700, "time_msc": t_msc, "last": 125000.0, "volume_real": 5.0, "flags": 32},
        {"time": 1791881700, "time_msc": t_msc, "last": 125000.0, "volume_real": 5.0, "flags": 32},
        {"time": 1791881700, "time_msc": t_msc, "last": 125000.0, "volume_real": 5.0, "flags": 32},
    ]

@pytest.fixture
def sample_synthetic_stream():
    """Gera sequência temporal de 50 ticks sintéticos distribuídos em 10 segundos."""
    base_msc = 1791881700000
    ticks = []
    base_price = 125000.0
    for i in range(50):
        msc = base_msc + (i * 200)
        ticks.append({
            "time": msc // 1000,
            "time_msc": msc,
            "last": base_price + (i % 5) * 5.0,
            "volume_real": 1.0 + (i % 3),
            "flags": 32 if i % 2 == 0 else 64,
        })
    return ticks
