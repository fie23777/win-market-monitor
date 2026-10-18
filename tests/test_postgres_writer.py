import pytest
from datetime import date
from src.storage.postgres_writer import PostgresMarketWriter, SafeDropViolationError
from src.storage.parquet_archiver import ParquetArchiver
from src.domain.calendar import get_last_n_b3_trading_days

def test_safe_drop_preserves_partitions_within_30_trading_days(tmp_path):
    """Comprova que pregões recentes (dentro dos últimos 30 pregões B3) nunca são dropados."""
    writer = PostgresMarketWriter(archiver=None)
    ref_date = date(2026, 10, 19)
    recent_trading_days = get_last_n_b3_trading_days(30, ref_date)
    
    # Pegar o 10º pregão mais recente
    day_to_test = recent_trading_days[10]
    res = writer.evaluate_safe_drop_partition(trading_date=day_to_test, retention_trading_days=30, reference_date=ref_date)

    assert res["action"] == "PRESERVE"
    assert "dentro do período de retenção ativo" in res["reason"]

def test_safe_drop_blocks_when_parquet_or_backup_is_missing(tmp_path):
    """Comprova que uma partição antiga (>30 pregões) é bloqueada se o Parquet ou backup faltarem."""
    archive_dir = tmp_path / "archive"
    backup_dir = tmp_path / "backup"
    archiver = ParquetArchiver(archive_dir=archive_dir, backup_dir=backup_dir)
    writer = PostgresMarketWriter(archiver=archiver)

    ref_date = date(2026, 10, 19)
    # Data muito antiga (ano 2025)
    old_date = date(2025, 5, 15)

    with pytest.raises(SafeDropViolationError, match="Arquivo Parquet ou manifest ausente"):
        writer.evaluate_safe_drop_partition(trading_date=old_date, retention_trading_days=30, reference_date=ref_date)
