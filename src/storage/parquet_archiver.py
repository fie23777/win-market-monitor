import json
import hashlib
import shutil
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date, datetime, timezone
from typing import List, Dict, Any, Optional
from src.domain.models import TradeTick, M2CandleClosed

class ParquetArchiver:
    """
    Gerenciador de arquivamento colunar Parquet com compressão ZSTD,
    auditoria por hash SHA-256 e replicação para destino de backup independente.
    """

    def __init__(self, archive_dir: str | Path, backup_dir: str | Path, compression_level: int = 7):
        self.archive_dir = Path(archive_dir)
        self.backup_dir = Path(backup_dir)
        self.compression_level = compression_level
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    def compute_file_sha256(self, file_path: Path) -> str:
        """Calcula o hash SHA-256 de um arquivo em disco."""
        sha = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                sha.update(chunk)
        return sha.hexdigest()

    def archive_ticks_day(
        self,
        trading_date: date,
        symbol: str,
        ticks: List[TradeTick]
    ) -> Dict[str, Any]:
        """
        Exporta os ticks de um pregão para Parquet/ZSTD, calcula SHA-256,
        replica para o backup independente e atualiza o manifesto de auditoria.
        """
        if not ticks:
            return {"status": "EMPTY", "rows": 0}

        # 1. Definir caminhos padronizados
        year_str = f"{trading_date.year:04d}"
        month_str = f"{trading_date.month:02d}"
        day_str = f"{trading_date.day:02d}"
        
        target_dir = self.archive_dir / "ticks" / f"year={year_str}" / f"month={month_str}" / f"day={day_str}"
        target_dir.mkdir(parents=True, exist_ok=True)
        parquet_file = target_dir / f"{symbol}_ticks.parquet"

        # 2. Construir PyArrow Table
        table = pa.Table.from_arrays([
            pa.array([t.symbol for t in ticks], type=pa.string()),
            pa.array([t.time_msc for t in ticks], type=pa.int64()),
            pa.array([t.tick_fingerprint for t in ticks], type=pa.string()),
            pa.array([t.time_utc for t in ticks], type=pa.timestamp('ms', tz='UTC')),
            pa.array([t.price for t in ticks], type=pa.float64()),
            pa.array([t.volume_real for t in ticks], type=pa.float64()),
            pa.array([t.flags for t in ticks], type=pa.int32()),
            pa.array([t.trade_side for t in ticks], type=pa.string()),
            pa.array([t.occurrence_ordinal for t in ticks], type=pa.int32()),
            pa.array([t.provenance for t in ticks], type=pa.string()),
        ], names=[
            "symbol", "time_msc", "tick_fingerprint", "time_utc",
            "price", "volume_real", "flags", "trade_side",
            "occurrence_ordinal", "provenance"
        ])

        # 3. Gravar com compressão Zstandard
        pq.write_table(
            table,
            parquet_file,
            compression="zstd",
            compression_level=self.compression_level
        )

        # 4. Validar integridade e calcular SHA-256
        sha256 = self.compute_file_sha256(parquet_file)
        read_back = pq.read_table(parquet_file)
        if len(read_back) != len(ticks):
            raise IOError(f"Falha de integridade: gravados {len(ticks)}, relidos {len(read_back)}")

        # 5. Replicar para destino de backup independente
        backup_target_dir = self.backup_dir / "ticks" / f"year={year_str}" / f"month={month_str}" / f"day={day_str}"
        backup_target_dir.mkdir(parents=True, exist_ok=True)
        backup_file = backup_target_dir / f"{symbol}_ticks.parquet"
        shutil.copy2(parquet_file, backup_file)

        backup_sha256 = self.compute_file_sha256(backup_file)
        if backup_sha256 != sha256:
            raise IOError("Falha na réplica de backup: Hash SHA-256 divergente!")

        # 6. Atualizar manifest.json
        manifest_path = target_dir / "manifest.json"
        manifest_data = {
            "symbol": symbol,
            "trading_date": trading_date.isoformat(),
            "parquet_file": str(parquet_file.name),
            "sha256": sha256,
            "rows_count": len(ticks),
            "total_volume": sum(t.volume_real for t in ticks),
            "min_time_utc": min(t.time_utc for t in ticks).isoformat(),
            "max_time_utc": max(t.time_utc for t in ticks).isoformat(),
            "provenance_distribution": {
                "REALTIME_STREAM": sum(1 for t in ticks if t.provenance == "REALTIME_STREAM"),
                "RECOVERED_HISTORICAL_MT5": sum(1 for t in ticks if t.provenance == "RECOVERED_HISTORICAL_MT5"),
            },
            "backup_file": str(backup_file.resolve()),
            "archived_at_utc": datetime.now(timezone.utc).isoformat()
        }

        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)

        return {
            "status": "ARCHIVED_AND_BACKED_UP",
            "parquet_file": str(parquet_file),
            "backup_file": str(backup_file),
            "sha256": sha256,
            "rows": len(ticks)
        }

    def validate_archive_integrity(self, parquet_path: Path, manifest_path: Path) -> bool:
        """Verifica se o Parquet existe, bate o hash do manifest e é legível."""
        if not parquet_path.exists() or not manifest_path.exists():
            return False

        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        actual_sha = self.compute_file_sha256(parquet_path)
        if actual_sha != manifest.get("sha256"):
            return False

        table = pq.read_table(parquet_path)
        return len(table) == manifest.get("rows_count")
