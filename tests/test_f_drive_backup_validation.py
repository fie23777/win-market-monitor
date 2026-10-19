import pytest
from pathlib import Path
from datetime import date, datetime, timezone
from src.domain.models import TradeTick
from src.storage.parquet_archiver import ParquetArchiver
from src.config import load_config

def test_parquet_backup_on_independent_f_drive(tmp_path):
    """
    Comprova que o procedimento de arquivamento Parquet exporta para o archive primário
    e replica com integridade criptográfica SHA-256 para o destino físico independente F:.
    """
    config = load_config()
    primary_archive = tmp_path / "primary_archive"
    f_backup_dir = Path(config.parquet.backup_dir) / "test_run"

    archiver = ParquetArchiver(
        archive_dir=primary_archive,
        backup_dir=f_backup_dir,
        compression_level=config.parquet.compression_level
    )

    t_utc = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
    trading_date = t_utc.date()

    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1792411200000 + i * 10,
            time_utc=t_utc,
            price=125000.0 + i,
            volume_real=1.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint=f"fp_backup_f_{i}",
            provenance="REALTIME_STREAM"
        )
        for i in range(10)
    ]

    try:
        res = archiver.archive_ticks_day(trading_date=trading_date, symbol="WINZ26", ticks=ticks)
        assert res["status"] == "ARCHIVED_AND_BACKED_UP"
        assert res["rows"] == 10

        primary_file = Path(res["parquet_file"])
        backup_file = Path(res["backup_file"])

        assert primary_file.exists(), "Arquivo Parquet primário não existe!"
        assert backup_file.exists(), "Arquivo Parquet de backup no F: não existe!"
        assert backup_file.stat().st_size == primary_file.stat().st_size

        # Validação do checksum SHA-256 entre primário e réplica no F:
        hash_primary = archiver.compute_file_sha256(primary_file)
        hash_backup = archiver.compute_file_sha256(backup_file)
        assert hash_primary == hash_backup == res["sha256"], "Divergência de hash entre primário e backup no F:!"
    finally:
        # Cleanup dos arquivos de teste gerados no F:\win-market-monitor-backup\test_run
        if f_backup_dir.exists():
            import shutil
            shutil.rmtree(f_backup_dir, ignore_errors=True)
