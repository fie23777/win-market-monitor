import pytest
from datetime import datetime, timezone, timedelta
from src.domain.models import TradeTick, M2CandleClosed
from src.storage.postgres_writer import PostgresMarketWriter

def test_real_postgres_connection_and_batch_write():
    """
    Teste Integrado Ponta a Ponta:
    Conecta no PostgreSQL 17 real no Docker (porta 5434), cria partição dinâmica,
    grava lote de TradeTicks e valida contagem e integridade.
    """
    writer = PostgresMarketWriter(host="127.0.0.1", port=5434, database="market_data", user="postgres", password="password")
    
    t_utc = datetime(2026, 10, 19, 14, 0, 0, tzinfo=timezone.utc)
    ticks = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791882100000 + i * 10,
            time_utc=t_utc + timedelta(milliseconds=i * 10),
            price=125100.0 + i * 5.0,
            volume_real=3.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint=f"fp_real_pg_{i}",
            provenance="REALTIME_STREAM"
        )
        for i in range(20)
    ]

    inserted = writer.write_ticks_batch(ticks)
    assert inserted == 20

    # Validar no banco real
    with writer.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND tick_fingerprint LIKE 'fp_real_pg_%';")
            count = cur.fetchone()[0]
            assert count == 20

def test_real_postgres_deduplication_on_conflict():
    """
    Comprova que a re-inserção de ticks já existentes é descartada
    pelo PostgreSQL via ON CONFLICT DO NOTHING sem duplicar dados.
    """
    writer = PostgresMarketWriter(host="127.0.0.1", port=5434, database="market_data", user="postgres", password="password")
    t_utc = datetime(2026, 10, 19, 14, 5, 0, tzinfo=timezone.utc)

    single_tick = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791882150000,
            time_utc=t_utc,
            price=125150.0,
            volume_real=5.0,
            flags=64,
            trade_side="SELL",
            occurrence_ordinal=1,
            tick_fingerprint="fp_dedup_real_1",
            provenance="REALTIME_STREAM"
        )
    ]

    writer.write_ticks_batch(single_tick)
    # Tentar reinserir o mesmo tick
    writer.write_ticks_batch(single_tick)

    with writer.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM market_ticks_raw WHERE tick_fingerprint = 'fp_dedup_real_1';")
            count = cur.fetchone()[0]
            assert count == 1, "Tick duplicado no PostgreSQL!"
