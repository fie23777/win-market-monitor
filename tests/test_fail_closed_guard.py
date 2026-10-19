import pytest
import psycopg2
from datetime import datetime, timezone, timedelta
from src.storage.postgres_writer import PostgresMarketWriter, OperationalDatabaseProtectionError
from src.domain.models import TradeTick

def test_writer_rejects_operational_database_under_test():
    """
    Teste Negativo 1: Comprova a proteção fail-closed.
    Qualquer tentativa de instanciar PostgresMarketWriter apontando para o banco operacional 'market_data'
    dentro do ambiente de testes deve ser imediatamente bloqueada com OperationalDatabaseProtectionError.
    """
    with pytest.raises(OperationalDatabaseProtectionError) as exc_info:
        PostgresMarketWriter(
            host="127.0.0.1",
            port=5434,
            database="market_data",
            user="postgres",
            password="password"
        )
    assert "PROTEÇÃO FAIL-CLOSED ATIVA" in str(exc_info.value)
    assert "market_data" in str(exc_info.value)

def test_connection_rejects_operational_database_under_test():
    """
    Teste Negativo 2: Mesmo que conn_params seja modificado em tempo de execução,
    get_connection() aplica segunda barreira fail-closed e recusa conectar a 'market_data'.
    """
    writer = PostgresMarketWriter(
        host="127.0.0.1",
        port=5434,
        database="market_data_test",
        user="postgres",
        password="password"
    )
    # Simula contaminação indevida de parâmetros
    writer.conn_params["dbname"] = "market_data"

    with pytest.raises(OperationalDatabaseProtectionError) as exc_info:
        writer.get_connection()
    assert "PROTEÇÃO FAIL-CLOSED ATIVA" in str(exc_info.value)

def test_writer_accepts_isolated_test_database(test_postgres_writer):
    """
    Comprova que o banco isolado 'market_data_test' conecta normalmente
    e está apto para suportar testes de integração.
    """
    with test_postgres_writer.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            current_db = cur.fetchone()[0]
            assert current_db == "market_data_test"

def test_isolated_database_does_not_affect_operational_database(test_postgres_writer):
    """
    Comprova o isolamento físico entre os bancos: dados gravados em 'market_data_test'
    não aparecem na partição do banco operacional 'market_data'.
    """
    t_utc = datetime(2026, 10, 19, 14, 0, 0, tzinfo=timezone.utc)
    tick = [
        TradeTick(
            symbol="WINZ26",
            time_msc=1791882100000,
            time_utc=t_utc,
            price=125100.0,
            volume_real=3.0,
            flags=32,
            trade_side="BUY",
            occurrence_ordinal=1,
            tick_fingerprint="fp_isolation_proof_1",
            provenance="REALTIME_STREAM"
        )
    ]

    # Grava no banco isolado de teste
    test_postgres_writer.write_ticks_batch(tick)

    # 1. Valida que está no market_data_test
    with test_postgres_writer.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM market_ticks_raw WHERE tick_fingerprint = 'fp_isolation_proof_1';")
            count_test = cur.fetchone()[0]
            assert count_test == 1

    # 2. Conecta diretamente no banco operacional (usando psycopg2 puro com autocommit para consulta de auditoria)
    # para comprovar que o operacional NÃO recebeu o registro
    raw_conn = psycopg2.connect(host="127.0.0.1", port=5434, dbname="market_data", user="postgres", password="password")
    try:
        with raw_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM market_ticks_raw WHERE tick_fingerprint = 'fp_isolation_proof_1';")
            count_operacional = cur.fetchone()[0]
            assert count_operacional == 0, "Violação de isolamento: registro de teste vazou para o banco operacional!"
    finally:
        raw_conn.close()
