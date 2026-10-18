import sqlite3
from src.domain.models import TradeTick, M2CandleClosed
from datetime import datetime, timezone, timedelta
from src.buffer.durable_spool import DurableSpool

def test_spool_pragmas_and_durability_mode(durable_spool):
    """Valida se o SQLite está configurado estritamente com WAL e synchronous=FULL (2)."""
    conn = durable_spool._get_connection()
    wal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
    sync_mode = conn.execute("PRAGMA synchronous;").fetchone()[0]
    conn.close()

    assert wal_mode.lower() == "wal"
    assert sync_mode == 2  # 2 = FULL

def test_atomic_batch_append_and_checkpoint_advance(durable_spool):
    """Comprova a escrita atômica em lotes e o avanço de last_captured_msc."""
    t_utc = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)
    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791881700100 + i * 10,
            time_utc=t_utc + timedelta(milliseconds=i * 10),
            price=125000.0,
            volume_real=1.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint=f"fp_{i}"
        )
        for i in range(10)
    ]

    inserted = durable_spool.append_ticks_batch(ticks, stream_key="WINZ26")
    assert inserted == 10

    ckp = durable_spool.get_checkpoints("WINZ26")
    assert ckp.last_captured_msc == 1791881700190
    assert ckp.last_replicated_msc == 0

    pending = durable_spool.get_pending_ticks(limit=100)
    assert len(pending) == 10

def test_crash_recovery_preserves_unreplicated_items(temp_spool_dir):
    """
    Simula encerramento abrupto do processo após a escrita local:
    Ao instanciar um novo objeto sobre o mesmo arquivo .db, todos os itens
    pendentes e checkpoints persistem íntegros.
    """
    db_file = temp_spool_dir / "crash_test.db"
    spool1 = DurableSpool(db_file)
    t_utc = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)

    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791881700500,
            time_utc=t_utc,
            price=125010.0,
            volume_real=2.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint="fp_crash_1"
        )
    ]
    spool1.append_ticks_batch(ticks, stream_key="WINZ26")

    # 'Crash' simulado: fechar spool1 sem chamar mark_ticks_replicated
    del spool1

    # Novo processo reinicia e reabre o arquivo
    spool2 = DurableSpool(db_file)
    pending = spool2.get_pending_ticks()
    assert len(pending) == 1
    assert pending[0].tick_fingerprint == "fp_crash_1"

    ckp = spool2.get_checkpoints("WINZ26")
    assert ckp.last_captured_msc == 1791881700500
    assert ckp.last_replicated_msc == 0
