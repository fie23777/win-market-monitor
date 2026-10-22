"""
Runner Oficial do Piloto Operacional Controlado de 5 Minutos.
win-market-monitor - WINZ26
"""

import sys
sys.path.insert(0, ".")
import os
import time
import json
import psutil
import logging
import sqlite3
import psycopg2
from datetime import datetime, timezone, timedelta

from src.config import load_config
from src.engine import MarketCollectorEngine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("pilot_5min_runner")

def run_pilot():
    config = load_config()
    engine = MarketCollectorEngine(config=config)
    
    process = psutil.Process()
    mem_peak_mb = process.memory_info().rss / (1024 * 1024)
    latencies_ms = []

    pilot_start_wall = datetime.now(timezone.utc)
    pilot_start_brt = pilot_start_wall.astimezone(engine.time_normalizer.market_tz)
    logger.info(f"=== INICIANDO PILOTO OPERACIONAL DE 5 MINUTOS ===")
    logger.info(f"Horário de Início: {pilot_start_brt.strftime('%Y-%m-%d %H:%M:%S %Z')} ({pilot_start_wall.isoformat()})")
    
    # 1. Inicialização e Recuperação Histórica (09:00 BRT até o momento)
    t0_init = time.time()
    init_res = engine.initialize_and_recover()
    t_init_sec = time.time() - t0_init
    logger.info(f"Recuperação inicial concluída em {t_init_sec:.2f}s: {init_res}")

    current_mem = process.memory_info().rss / (1024 * 1024)
    if current_mem > mem_peak_mb:
        mem_peak_mb = current_mem

    # 2. Loop de streaming em tempo real durante 300 segundos
    target_duration_sec = 300.0 # 5 minutos exatos
    t_start = time.time()
    poll_sec = config.collector.poll_interval_ms / 1000.0
    
    captured_realtime_total = 0
    replicated_realtime_total = 0
    cycle_count = 0

    logger.info(f"Iniciando loop de streaming contínuo por {target_duration_sec} segundos...")
    while (time.time() - t_start) < target_duration_sec:
        c_t0 = time.time()
        
        # Captura no SQLite
        captured = engine.capture_step()
        captured_realtime_total += captured
        
        # Replicação para o PostgreSQL
        replicated = engine.replicate_pending_to_postgres(batch_size=config.buffer.batch_size)
        replicated_realtime_total += replicated
        
        # Medição de latência do ciclo
        c_latency = (time.time() - c_t0) * 1000.0
        latencies_ms.append(c_latency)
        cycle_count += 1

        # Medição de memória
        c_mem = process.memory_info().rss / (1024 * 1024)
        if c_mem > mem_peak_mb:
            mem_peak_mb = c_mem

        # Log de batimento a cada 30 segundos
        elapsed = time.time() - t_start
        if cycle_count % 150 == 0:
            logger.info(f"Progresso: {elapsed:.1f}s / {target_duration_sec}s | Ciclos: {cycle_count} | Capturados RT: {captured_realtime_total} | Replicados RT: {replicated_realtime_total} | RAM: {c_mem:.1f}MB")

        time.sleep(poll_sec)

    logger.info(f"Período de 5 minutos atingido. Iniciando encerramento gracioso...")
    
    # 3. Drenagem final
    final_drained = 0
    while True:
        d = engine.replicate_pending_to_postgres(batch_size=5000)
        if d == 0:
            break
        final_drained += d
    
    # Sincronização final de candles M2
    try:
        final_m2 = engine.sync_m2_candles()
    except Exception as e:
        final_m2 = 0
        logger.warning(f"Aviso sync M2 final: {e}")

    # Encerramento seguro da conexão MT5
    engine.shutdown()
    
    pilot_end_wall = datetime.now(timezone.utc)
    pilot_end_brt = pilot_end_wall.astimezone(engine.time_normalizer.market_tz)
    logger.info(f"Piloto encerrado graciosamente às {pilot_end_brt.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # 4. Auditoria e Reconciliação dos Dados Persistidos
    # SQLite
    conn_sq = sqlite3.connect(config.buffer.sqlite_path)
    cur_sq = conn_sq.cursor()
    cur_sq.execute("SELECT count(*) FROM spool_ticks;")
    spool_total_ticks = cur_sq.fetchone()[0]
    cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE status = 'COMMITTED';")
    spool_committed_ticks = cur_sq.fetchone()[0]
    cur_sq.execute("SELECT count(*) FROM spool_ticks WHERE status = 'PENDING';")
    spool_pending_ticks = cur_sq.fetchone()[0]
    cur_sq.execute("SELECT count(*) FROM spool_candles_m2;")
    spool_candles_total = cur_sq.fetchone()[0]
    cur_sq.execute("SELECT last_captured_msc, last_replicated_msc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
    ckp_row = cur_sq.fetchone()
    ckp_captured_msc = ckp_row[0] if ckp_row else 0
    ckp_replicated_msc = ckp_row[1] if ckp_row else 0
    conn_sq.close()

    # PostgreSQL
    conn_pg = psycopg2.connect(
        host=config.postgres.host,
        port=config.postgres.port,
        dbname=config.postgres.database,
        user=config.postgres.user,
        password=config.postgres.password
    )
    cur_pg = conn_pg.cursor()
    cur_pg.execute("SELECT count(*) FROM market_ticks_raw WHERE time_utc >= '2026-10-19 00:00:00+00';")
    pg_ticks_today = cur_pg.fetchone()[0]
    cur_pg.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'RECOVERED_HISTORICAL_MT5';")
    pg_recovered_hist = cur_pg.fetchone()[0]
    cur_pg.execute("SELECT count(*) FROM market_ticks_raw WHERE provenance = 'REALTIME_STREAM';")
    pg_realtime = cur_pg.fetchone()[0]
    cur_pg.execute("SELECT count(*) FROM candles_m2 WHERE open_time_utc >= '2026-10-19 00:00:00+00';")
    pg_candles_today = cur_pg.fetchone()[0]
    
    # Primeiro e último tick no PostgreSQL
    cur_pg.execute("""
    SELECT time_msc, time_utc, price, volume_real, provenance
    FROM market_ticks_raw
    WHERE time_utc >= '2026-10-19 00:00:00+00'
    ORDER BY time_utc ASC, occurrence_ordinal ASC
    LIMIT 1;
    """)
    first_row = cur_pg.fetchone()

    cur_pg.execute("""
    SELECT time_msc, time_utc, price, volume_real, provenance
    FROM market_ticks_raw
    WHERE time_utc >= '2026-10-19 00:00:00+00'
    ORDER BY time_utc DESC, occurrence_ordinal DESC
    LIMIT 1;
    """)
    last_row = cur_pg.fetchone()
    conn_pg.close()

    avg_latency = sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0.0
    max_latency = max(latencies_ms) if latencies_ms else 0.0

    report_data = {
        "pilot_start_brt": pilot_start_brt.isoformat(),
        "pilot_end_brt": pilot_end_brt.isoformat(),
        "pilot_start_utc": pilot_start_wall.isoformat(),
        "pilot_end_utc": pilot_end_wall.isoformat(),
        "duration_seconds": time.time() - t_start,
        "init_recovery_seconds": t_init_sec,
        "recovered_historical_count": init_res.get("recovered_count", 0),
        "candles_m2_recovered": init_res.get("candles_m2_count", 0),
        "realtime_captured": captured_realtime_total,
        "realtime_replicated": replicated_realtime_total,
        "spool_total_ticks": spool_total_ticks,
        "spool_committed_ticks": spool_committed_ticks,
        "spool_pending_ticks": spool_pending_ticks,
        "spool_candles_total": spool_candles_total,
        "ckp_last_captured_msc": ckp_captured_msc,
        "ckp_last_replicated_msc": ckp_replicated_msc,
        "pg_ticks_today": pg_ticks_today,
        "pg_recovered_hist": pg_recovered_hist,
        "pg_realtime": pg_realtime,
        "pg_candles_today": pg_candles_today,
        "first_tick": {
            "time_msc": first_row[0] if first_row else None,
            "time_utc": first_row[1].isoformat() if first_row else None,
            "time_brt": first_row[1].astimezone(engine.time_normalizer.market_tz).isoformat() if first_row else None,
            "price": first_row[2] if first_row else None,
            "volume_real": first_row[3] if first_row else None,
            "provenance": first_row[4] if first_row else None,
        } if first_row else None,
        "last_tick": {
            "time_msc": last_row[0] if last_row else None,
            "time_utc": last_row[1].isoformat() if last_row else None,
            "time_brt": last_row[1].astimezone(engine.time_normalizer.market_tz).isoformat() if last_row else None,
            "price": last_row[2] if last_row else None,
            "volume_real": last_row[3] if last_row else None,
            "provenance": last_row[4] if last_row else None,
        } if last_row else None,
        "peak_ram_mb": mem_peak_mb,
        "avg_cycle_latency_ms": avg_latency,
        "max_cycle_latency_ms": max_latency,
        "reconciliation_discrepancy": spool_committed_ticks - pg_ticks_today
    }

    out_file = Path("C:/Users/fie23/.gemini/antigravity-ide/brain/ef92ffd5-2a0e-47f2-97b3-b07361021f8d/pilot_5min_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2, ensure_ascii=False)
    
    print("\n=== RESUMO EXECUTIVO DO PILOTO ===")
    print(f"Duração: {report_data['duration_seconds']:.2f} segundos")
    print(f"Total Ticks SQLite Spool: {spool_total_ticks} (Committed: {spool_committed_ticks}, Pending: {spool_pending_ticks})")
    print(f"Total Ticks PostgreSQL:   {pg_ticks_today} (Histórico: {pg_recovered_hist}, Tempo Real: {pg_realtime})")
    print(f"Total Candles M2 PG:      {pg_candles_today}")
    print(f"Checkpoints: Captured={ckp_captured_msc} | Replicated={ckp_replicated_msc} (Diff={ckp_captured_msc - ckp_replicated_msc})")
    print(f"Pico de Memória RAM:      {mem_peak_mb:.2f} MB")
    print(f"Latência Média por Ciclo: {avg_latency:.2f} ms (Máxima: {max_latency:.2f} ms)")
    print(f"Discrepância Spool vs PG: {report_data['reconciliation_discrepancy']}")

if __name__ == "__main__":
    run_pilot()
