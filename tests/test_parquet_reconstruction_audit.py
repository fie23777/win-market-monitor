from datetime import date, datetime, timezone, timedelta
from src.domain.models import TradeTick
from src.storage.postgres_writer import PostgresMarketWriter
from src.storage.parquet_archiver import ParquetArchiver

def test_parquet_reconstruction_and_cross_audit_from_postgres(tmp_path, test_postgres_writer):
    """
    Comprova que os dados gravados no PostgreSQL 17 isolado de teste (market_data_test)
    podem ser exportados para Parquet/ZSTD com manifest SHA-256 e auditados com correspondência de 100%.
    """
    writer = test_postgres_writer
    archive_dir = tmp_path / "archive"
    backup_dir = tmp_path / "backup"
    archiver = ParquetArchiver(archive_dir=archive_dir, backup_dir=backup_dir, compression_level=7)

    t_utc = datetime(2026, 10, 19, 16, 0, 0, tzinfo=timezone.utc)
    trading_date = t_utc.date()

    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791882500000 + i * 5,
            time_utc=t_utc + timedelta(milliseconds=i * 5),
            price=125400.0 + i,
            volume_real=10.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint=f"fp_audit_recon_{i}",
            provenance="REALTIME_STREAM"
        )
        for i in range(25)
    ]

    try:
        # Inserir no PostgreSQL 17 real
        writer.write_ticks_batch(ticks)

        # Reconstruir os ticks lendo diretamente do PostgreSQL
        with writer.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                SELECT symbol, time_msc, tick_fingerprint, time_utc, price, volume_real, flags, trade_side, occurrence_ordinal, provenance
                FROM market_ticks_raw
                WHERE tick_fingerprint LIKE 'fp_audit_recon_%'
                ORDER BY time_msc ASC, occurrence_ordinal ASC;
                """)
                rows = cur.fetchall()

        reconstructed_ticks = [
            TradeTick(
                symbol=r[0], time_msc=r[1], tick_fingerprint=r[2], time_utc=r[3],
                price=r[4], volume_real=r[5], flags=r[6], trade_side=r[7],
                occurrence_ordinal=r[8], provenance=r[9]
            )
            for r in rows
        ]
        assert len(reconstructed_ticks) == 25

        # Exportar para Parquet ZSTD e gerar manifest
        res = archiver.archive_ticks_day(trading_date=trading_date, symbol="WINZ26", ticks=reconstructed_ticks)
        assert res["status"] == "ARCHIVED_AND_BACKED_UP"
        assert res["rows"] == 25

        parquet_file = archiver.archive_dir / f"ticks/year={trading_date.year:04d}/month={trading_date.month:02d}/day={trading_date.day:02d}/WINZ26_ticks.parquet"
        manifest_file = archiver.archive_dir / f"ticks/year={trading_date.year:04d}/month={trading_date.month:02d}/day={trading_date.day:02d}/manifest.json"

        assert archiver.validate_archive_integrity(parquet_file, manifest_file) is True
    finally:
        with writer.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM market_ticks_raw WHERE tick_fingerprint LIKE 'fp_audit_recon_%';")
            conn.commit()
