import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional
from src.domain.models import TradeTick, M2CandleClosed, Checkpoints
from src.connection.mt5_time_normalizer import MT5TimeNormalizer

class DurableSpool:
    """
    Buffer local transacional e durável em disco utilizando SQLite WAL e synchronous=FULL.
    Gerencia checkpoints de dois estágios:
    - last_captured_msc: Avança após o commit durável no SQLite local.
    - last_replicated_msc: Avança após a confirmação de persistência remota no PostgreSQL.
    """

    def __init__(self, db_path: str | Path, time_normalizer: Optional[MT5TimeNormalizer] = None):
        self.db_path = Path(db_path)
        self.time_normalizer = time_normalizer or MT5TimeNormalizer()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = FULL;")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS spool_ticks (
                tick_fingerprint TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                time_msc INTEGER NOT NULL,
                price REAL NOT NULL,
                volume_real REAL NOT NULL,
                flags INTEGER NOT NULL,
                trade_side TEXT NOT NULL,
                occurrence_ordinal INTEGER NOT NULL,
                provenance TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                created_at_utc TEXT NOT NULL
            );
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_spool_status ON spool_ticks (status, time_msc);
            """)
            conn.execute("""
            CREATE TABLE IF NOT EXISTS spool_candles_m2 (
                symbol TEXT NOT NULL,
                open_time_msc INTEGER NOT NULL,
                close_time_msc INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume_real REAL NOT NULL,
                tick_volume INTEGER NOT NULL,
                spread INTEGER NOT NULL,
                provenance TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                created_at_utc TEXT NOT NULL,
                PRIMARY KEY (symbol, open_time_msc)
            );
            """)
            conn.execute("""
            CREATE TABLE IF NOT EXISTS checkpoints_two_stage (
                stream_key TEXT PRIMARY KEY,
                last_captured_msc INTEGER NOT NULL DEFAULT 0,
                last_replicated_msc INTEGER NOT NULL DEFAULT 0,
                updated_at_utc TEXT NOT NULL
            );
            """)

    def append_ticks_batch(self, ticks: List[TradeTick], stream_key: str = "WINZ26") -> int:
        """
        Persiste atômica e duravelmente um lote de ticks no buffer local e avança o checkpoint de captura.
        """
        if not ticks:
            return 0

        now_iso = datetime.now(timezone.utc).isoformat()
        max_msc = max(t.time_msc for t in ticks)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE;")
            
            records = [
                (
                    t.tick_fingerprint, t.symbol, t.time_msc, t.price, t.volume_real,
                    t.flags, t.trade_side, t.occurrence_ordinal, t.provenance, now_iso
                )
                for t in ticks
            ]
            cursor.executemany("""
            INSERT OR IGNORE INTO spool_ticks (
                tick_fingerprint, symbol, time_msc, price, volume_real,
                flags, trade_side, occurrence_ordinal, provenance, status, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?);
            """, records)

            # Atualizar checkpoint de captura local
            cursor.execute("""
            INSERT INTO checkpoints_two_stage (stream_key, last_captured_msc, last_replicated_msc, updated_at_utc)
            VALUES (?, ?, 0, ?)
            ON CONFLICT(stream_key) DO UPDATE SET
                last_captured_msc = MAX(last_captured_msc, excluded.last_captured_msc),
                updated_at_utc = excluded.updated_at_utc;
            """, (stream_key, max_msc, now_iso))

            conn.commit()
            return len(records)

    def append_candle_m2(self, candle: M2CandleClosed) -> bool:
        """Persiste duravelmente um candle M2 fechado no spool local."""
        candle.validate()
        now_iso = datetime.now(timezone.utc).isoformat()
        open_msc = int(candle.open_time_utc.timestamp() * 1000)
        close_msc = int(candle.close_time_utc.timestamp() * 1000)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT OR REPLACE INTO spool_candles_m2 (
                symbol, open_time_msc, close_time_msc, open, high, low, close,
                volume_real, tick_volume, spread, provenance, status, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?);
            """, (
                candle.symbol, open_msc, close_msc, candle.open, candle.high,
                candle.low, candle.close, candle.volume_real, candle.tick_volume,
                candle.spread, candle.provenance, now_iso
            ))
            conn.commit()
            return True

    def get_pending_ticks(self, limit: int = 1000) -> List[TradeTick]:
        """Recupera ticks pendentes de replicação remota."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT tick_fingerprint, symbol, time_msc, price, volume_real,
                   flags, trade_side, occurrence_ordinal, provenance, created_at_utc
            FROM spool_ticks
            WHERE status = 'PENDING'
            ORDER BY time_msc ASC, occurrence_ordinal ASC
            LIMIT ?;
            """, (limit,))
            rows = cursor.fetchall()

        ticks = []
        for r in rows:
            t_utc = self.time_normalizer.server_msc_to_utc_datetime(r["time_msc"])
            ticks.append(TradeTick(
                symbol=r["symbol"],
                time_msc=r["time_msc"],
                time_utc=t_utc,
                price=r["price"],
                volume_real=r["volume_real"],
                flags=r["flags"],
                trade_side=r["trade_side"],
                occurrence_ordinal=r["occurrence_ordinal"],
                tick_fingerprint=r["tick_fingerprint"],
                provenance=r["provenance"],
            ))
        return ticks

    def mark_ticks_replicated(self, fingerprints: List[str], max_replicated_msc: int, stream_key: str = "WINZ26"):
        """Avança o checkpoint de replicação remota após confirmação do PostgreSQL."""
        if not fingerprints:
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE;")
            
            # Atualiza status em blocos limitados para evitar limite de parâmetros SQLite
            chunk_size = 500
            for i in range(0, len(fingerprints), chunk_size):
                chunk = fingerprints[i:i + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                cursor.execute(f"""
                UPDATE spool_ticks SET status = 'COMMITTED'
                WHERE tick_fingerprint IN ({placeholders});
                """, chunk)

            cursor.execute("""
            INSERT INTO checkpoints_two_stage (stream_key, last_captured_msc, last_replicated_msc, updated_at_utc)
            VALUES (?, 0, ?, ?)
            ON CONFLICT(stream_key) DO UPDATE SET
                last_replicated_msc = MAX(last_replicated_msc, excluded.last_replicated_msc),
                updated_at_utc = excluded.updated_at_utc;
            """, (stream_key, max_replicated_msc, now_iso))

            conn.commit()

    def get_checkpoints(self, stream_key: str = "WINZ26") -> Checkpoints:
        """Retorna os checkpoints de dois estágios atuais."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            SELECT stream_key, last_captured_msc, last_replicated_msc, updated_at_utc
            FROM checkpoints_two_stage
            WHERE stream_key = ?;
            """, (stream_key,))
            row = cursor.fetchone()

        if row:
            upd = datetime.fromisoformat(row["updated_at_utc"])
            return Checkpoints(
                stream_key=row["stream_key"],
                last_captured_msc=row["last_captured_msc"],
                last_replicated_msc=row["last_replicated_msc"],
                updated_at_utc=upd
            )
        return Checkpoints(stream_key=stream_key, last_captured_msc=0, last_replicated_msc=0)
