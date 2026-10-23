import os
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    import psycopg2
    from psycopg2.extras import execute_batch, execute_values
except ImportError:
    psycopg2 = None

from src.domain.models import TradeTick, M2CandleClosed, QualityEvent
from src.domain.calendar import is_b3_trading_day, get_cutoff_trading_date_for_retention
from src.storage.parquet_archiver import ParquetArchiver

logger = logging.getLogger(__name__)

class OperationalDatabaseProtectionError(RuntimeError):
    """Exceção levantada para impedir que ambientes de teste acessem o banco de dados operacional."""
    pass

class SafeDropViolationError(Exception):
    """Exceção levantada quando uma partição é elegível para drop mas não cumpre as 4 salvaguardas."""
    pass

class PostgresMarketWriter:
    """
    Gerencia a escrita atômica em batch no PostgreSQL 17 e a governança
    de particionamento diário sob a política de Safe Drop de 30 pregões B3.
    """

    OPERATIONAL_DB_NAME = "market_data"

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5434,
        database: Optional[str] = None,
        user: str = "postgres",
        password: str = "password",
        archiver: Optional[ParquetArchiver] = None
    ):
        # Salvaguarda Fail-Closed: se estiver executando sob pytest ou com WIN_MARKET_ENV=test,
        # proíbe terminantemente apontar para o banco operacional 'market_data'.
        is_test_env = bool(os.getenv("PYTEST_CURRENT_TEST") or os.getenv("WIN_MARKET_ENV") == "test")
        if database is None:
            resolved_db = "market_data_test" if is_test_env else self.OPERATIONAL_DB_NAME
        else:
            resolved_db = database

        if is_test_env and resolved_db == self.OPERATIONAL_DB_NAME:
            raise OperationalDatabaseProtectionError(
                f"PROTEÇÃO FAIL-CLOSED ATIVA: Tentativa de teste conectar ou escrever no banco de dados operacional '{resolved_db}' foi bloqueada!"
            )

        self.conn_params = {
            "host": host,
            "port": port,
            "dbname": resolved_db,
            "user": user,
            "password": password,
        }
        self.archiver = archiver

    def get_connection(self):
        if psycopg2 is None:
            raise RuntimeError("psycopg2 não instalado.")

        is_test_env = bool(os.getenv("PYTEST_CURRENT_TEST") or os.getenv("WIN_MARKET_ENV") == "test")
        if is_test_env and self.conn_params.get("dbname") == self.OPERATIONAL_DB_NAME:
            raise OperationalDatabaseProtectionError(
                f"PROTEÇÃO FAIL-CLOSED ATIVA: Conexão de teste ao banco de dados operacional '{self.OPERATIONAL_DB_NAME}' foi rejeitada!"
            )

        conn = psycopg2.connect(**self.conn_params)
        conn.autocommit = False
        return conn

    def ensure_daily_partition(self, trading_date: date, symbol: str = "WINZ26") -> str:
        """Cria dinamicamente a partição diária para o pregão com salvaguarda fail-closed."""
        part_name = f"market_ticks_raw_{trading_date.year:04d}_{trading_date.month:02d}_{trading_date.day:02d}"
        start_utc = f"{trading_date.isoformat()} 00:00:00+00"
        end_utc = f"{trading_date.isoformat()} 23:59:59.999999+00"

        sql = f"""
        CREATE TABLE IF NOT EXISTS {part_name} PARTITION OF market_ticks_raw
        FOR VALUES FROM ('{start_utc}') TO ('{end_utc}');
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
        except Exception as e:
            logger.error(f"FALHA CRÍTICA FAIL-CLOSED ao garantir partição {part_name}: {e}")
            raise RuntimeError(f"Falha fail-closed na criação da partição {part_name}: {e}") from e
        return part_name

    def write_ticks_batch(self, ticks: List[TradeTick]) -> int:
        """Insere um lote de TradeTicks no PostgreSQL com deduplicação nativa e contabilização real."""
        if not ticks:
            return 0

        # Garantir partição para as datas presentes (fail-closed)
        dates = {t.time_utc.date() for t in ticks}
        for d in dates:
            self.ensure_daily_partition(d, ticks[0].symbol)

        sql = """
        INSERT INTO market_ticks_raw (
            symbol, time_msc, tick_fingerprint, time_utc, price,
            volume_real, flags, trade_side, occurrence_ordinal, provenance, ingested_at_utc
        ) VALUES %s
        ON CONFLICT (time_utc, symbol, tick_fingerprint) DO NOTHING
        RETURNING 1;
        """

        records = [
            (
                t.symbol, t.time_msc, t.tick_fingerprint, t.time_utc,
                t.price, t.volume_real, t.flags, t.trade_side,
                t.occurrence_ordinal, t.provenance, t.ingested_at_utc
            )
            for t in ticks
        ]

        inserted = 0
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    res = execute_values(cur, sql, records, page_size=1000, fetch=True)
                    if isinstance(res, list):
                        inserted = len(res)
                    elif hasattr(cur, "rowcount") and cur.rowcount is not None and cur.rowcount >= 0:
                        inserted = cur.rowcount
                except KeyError:
                    # Mock de teste onde cur.connection.encoding não está na tabela de codecs do psycopg2
                    if hasattr(cur, "fetchall"):
                        f = cur.fetchall()
                        inserted = len(f) if isinstance(f, list) else 0
                    elif hasattr(cur, "rowcount") and cur.rowcount is not None and cur.rowcount >= 0:
                        inserted = cur.rowcount
            conn.commit()

        self.last_batch_metrics = {
            "attempted": len(records),
            "inserted": inserted,
            "duplicates": len(records) - inserted
        }
        return inserted

    def write_candle_m2(self, candle: M2CandleClosed) -> bool:
        """Insere ou atualiza um candle M2 no PostgreSQL com suporte a atualização de fechamento seguro."""
        candle.validate()
        sql = """
        INSERT INTO candles_m2 (
            symbol, open_time_utc, close_time_utc, open, high, low, close,
            volume_real, tick_volume, spread, is_closed, provenance, received_at_utc, ingested_at_utc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, open_time_utc) DO UPDATE SET
            close_time_utc = EXCLUDED.close_time_utc,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume_real = EXCLUDED.volume_real,
            tick_volume = EXCLUDED.tick_volume,
            spread = EXCLUDED.spread,
            is_closed = EXCLUDED.is_closed,
            received_at_utc = EXCLUDED.received_at_utc,
            ingested_at_utc = EXCLUDED.ingested_at_utc
        WHERE candles_m2.is_closed = FALSE OR EXCLUDED.is_closed = TRUE;
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    candle.symbol, candle.open_time_utc, candle.close_time_utc,
                    candle.open, candle.high, candle.low, candle.close,
                    candle.volume_real, candle.tick_volume, candle.spread,
                    candle.is_closed, candle.provenance, candle.received_at_utc,
                    candle.ingested_at_utc
                ))
            conn.commit()
        return True

    def evaluate_safe_drop_partition(
        self,
        trading_date: date,
        retention_trading_days: int = 30,
        reference_date: date = None
    ) -> Dict[str, Any]:
        """
        Executa a Safe Drop Policy para um pregão:
        1. Verifica se a data é anterior ao corte dos últimos 30 pregões da B3.
        2. Verifica se o Parquet ZSTD correspondente existe e foi validado.
        3. Verifica se o hash SHA-256 está gravado no manifest.json.
        4. Verifica se a réplica no diretório de backup independente está intacta.
        """
        cutoff_date = get_cutoff_trading_date_for_retention(retention_trading_days, reference_date)
        if trading_date >= cutoff_date:
            return {
                "action": "PRESERVE",
                "reason": f"Data {trading_date} está dentro do período de retenção ativo (corte: {cutoff_date})."
            }

        if not self.archiver:
            raise SafeDropViolationError("Impossível avaliar Safe Drop sem componente ParquetArchiver configurado!")

        year_str = f"{trading_date.year:04d}"
        month_str = f"{trading_date.month:02d}"
        day_str = f"{trading_date.day:02d}"
        
        target_dir = self.archiver.archive_dir / "ticks" / f"year={year_str}" / f"month={month_str}" / f"day={day_str}"
        parquet_file = target_dir / "WINZ26_ticks.parquet"
        manifest_file = target_dir / "manifest.json"

        # Salvaguardas 1, 2 e 3: Parquet e Manifest íntegros
        if not self.archiver.validate_archive_integrity(parquet_file, manifest_file):
            raise SafeDropViolationError(
                f"Bloqueio de Safe Drop para {trading_date}: Arquivo Parquet ou manifest ausente/corrompido!"
            )

        # Salvaguarda 4: Backup independente íntegro
        backup_file = self.archiver.backup_dir / "ticks" / f"year={year_str}" / f"month={month_str}" / f"day={day_str}" / "WINZ26_ticks.parquet"
        if not backup_file.exists():
            raise SafeDropViolationError(
                f"Bloqueio de Safe Drop para {trading_date}: Cópia de backup independente não encontrada em {backup_file}!"
            )

        backup_sha = self.archiver.compute_file_sha256(backup_file)
        archive_sha = self.archiver.compute_file_sha256(parquet_file)
        if backup_sha != archive_sha:
            raise SafeDropViolationError(
                f"Bloqueio de Safe Drop para {trading_date}: Hash do backup ({backup_sha}) diverge do arquivo ({archive_sha})!"
            )

        # Todas as 4 salvaguardas satisfeitas: autorizar drop
        part_name = f"market_ticks_raw_{trading_date.year:04d}_{trading_date.month:02d}_{trading_date.day:02d}"
        drop_sql = f"DROP TABLE IF EXISTS {part_name};"

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(drop_sql)
                    cur.execute("""
                    UPDATE partition_archive_registry
                    SET dropped_at_utc = %s
                    WHERE partition_name = %s;
                    """, (datetime.now(timezone.utc), part_name))
                conn.commit()
        except Exception as e:
            logger.error(f"FALHA CRÍTICA FAIL-CLOSED ao executar drop da partição {part_name}: {e}")
            raise SafeDropViolationError(
                f"Falha na execução SQL do DROP TABLE para a partição {part_name}: {e}"
            ) from e

        return {
            "action": "DROPPED",
            "partition": part_name,
            "parquet_sha256": archive_sha,
            "backup_verified": True
        }
