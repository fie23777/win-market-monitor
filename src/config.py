import os
import yaml
from pathlib import Path
from dataclasses import dataclass

@dataclass
class BufferConfig:
    sqlite_path: str
    journal_mode: str
    synchronous: str
    batch_size: int

@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    retention_trading_days: int

@dataclass
class ParquetConfig:
    archive_dir: str
    backup_dir: str
    compression: str
    compression_level: int
    ticks_retention_years: int
    candles_m2_retention_years: int

@dataclass
class CollectorSettings:
    read_only: bool
    allow_orders: bool
    poll_interval_ms: int
    gap_inactivity_threshold_seconds: int

@dataclass
class AppConfig:
    symbol: str
    timezones: dict
    buffer: BufferConfig
    postgres: PostgresConfig
    parquet: ParquetConfig
    collector: CollectorSettings

def load_config(config_path: str | Path = None) -> AppConfig:
    if config_path is None:
        base_dir = Path(__file__).resolve().parent.parent
        config_path = base_dir / "config" / "collector_config.yaml"
    
    with open(config_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
        
    return AppConfig(
        symbol=data.get("symbol", "WINZ26"),
        timezones=data.get("timezones", {"storage": "UTC", "display": "America/Sao_Paulo"}),
        buffer=BufferConfig(**data.get("buffer", {})),
        postgres=PostgresConfig(**data.get("postgres", {})),
        parquet=ParquetConfig(**data.get("parquet", {})),
        collector=CollectorSettings(**data.get("collector", {})),
    )
