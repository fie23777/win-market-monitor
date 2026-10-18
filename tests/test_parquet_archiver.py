import pyarrow.parquet as pq
from datetime import date, datetime, timezone
from src.domain.models import TradeTick
from src.storage.parquet_archiver import ParquetArchiver

def test_parquet_zstd_compression_and_backup_replication(tmp_path):
    """
    Comprova que o arquivo Parquet é comprimido com Zstandard,
    possui manifest SHA-256 e foi replicado com sucesso no backup independente.
    """
    archive_dir = tmp_path / "archive"
    backup_dir = tmp_path / "independent_backup_drive"
    archiver = ParquetArchiver(archive_dir=archive_dir, backup_dir=backup_dir, compression_level=7)

    trading_date = date(2026, 10, 19)
    t_utc = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)

    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791882000000 + i * 100,
            time_utc=t_utc,
            price=125000.0 + i,
            volume_real=5.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint=f"fp_{i}",
            provenance="REALTIME_STREAM"
        )
        for i in range(100)
    ]

    res = archiver.archive_ticks_day(trading_date=trading_date, symbol="WINZ26", ticks=ticks)
    assert res["status"] == "ARCHIVED_AND_BACKED_UP"
    assert res["rows"] == 100

    parquet_file = archive_dir / "ticks" / "year=2026" / "month=10" / "day=19" / "WINZ26_ticks.parquet"
    manifest_file = archive_dir / "ticks" / "year=2026" / "month=10" / "day=19" / "manifest.json"
    backup_file = backup_dir / "ticks" / "year=2026" / "month=10" / "day=19" / "WINZ26_ticks.parquet"

    assert parquet_file.exists()
    assert manifest_file.exists()
    assert backup_file.exists()

    # Validar que a compressão usada é ZSTD
    parquet_metadata = pq.read_metadata(parquet_file)
    assert parquet_metadata.row_group(0).column(0).compression.lower() == "zstd"

    # Validar integridade e correspondência de hash
    assert archiver.validate_archive_integrity(parquet_file, manifest_file) is True
    assert archiver.compute_file_sha256(parquet_file) == archiver.compute_file_sha256(backup_file)
