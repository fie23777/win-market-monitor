import pytest
from pathlib import Path
from datetime import date, datetime, timezone
from src.domain.models import TradeTick
from src.storage.parquet_archiver import ParquetArchiver
from src.storage.postgres_writer import PostgresMarketWriter, SafeDropViolationError

def test_independent_backup_device_configuration_and_corruption_detection(tmp_path):
    """
    Comprova que o diretório de backup independente pode residir em outro drive/local
    e que qualquer divergência no backup bloqueia compulsoriamente o Safe Drop.
    """
    primary_archive = tmp_path / "primary_storage"
    secondary_backup_drive = tmp_path / "secondary_isolated_drive"

    archiver = ParquetArchiver(
        archive_dir=primary_archive,
        backup_dir=secondary_backup_drive,
        compression_level=7
    )
    writer = PostgresMarketWriter(archiver=archiver)

    old_date = date(2025, 6, 10)
    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1750000000000,
            time_utc=datetime(2025, 6, 10, 14, 0, 0, tzinfo=timezone.utc),
            price=120000.0,
            volume_real=1.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint="fp_backup_1"
        )
    ]

    # Arquivar e criar backup
    archiver.archive_ticks_day(trading_date=old_date, symbol="WINZ26", ticks=ticks)

    # 1. Safe Drop deve passar quando ambos estão intactos
    res = writer.evaluate_safe_drop_partition(trading_date=old_date, retention_trading_days=30, reference_date=date(2026, 10, 19))
    assert res["action"] == "DROPPED"
    assert res["backup_verified"] is True

    # 2. Corromper o arquivo de backup e comprovar bloqueio imediato
    backup_file = secondary_backup_drive / "ticks/year=2025/month=06/day=10/WINZ26_ticks.parquet"
    with open(backup_file, "ab") as f:
        f.write(b"CORRUPTED_BYTES")

    with pytest.raises(SafeDropViolationError, match="Hash do backup .* diverge do arquivo"):
        writer.evaluate_safe_drop_partition(trading_date=old_date, retention_trading_days=30, reference_date=date(2026, 10, 19))
