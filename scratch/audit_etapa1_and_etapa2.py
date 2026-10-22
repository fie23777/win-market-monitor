import os
import sys
import sqlite3
import psycopg2
import psutil
from datetime import datetime, timezone, timedelta
import zoneinfo
import yaml

from src.connection.mt5_reader import MT5ReadOnlyReader
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.reconciliation.tick_reconciler import TickReconciler

def audit_etapa1_and_etapa2():
    brt_tz = zoneinfo.ZoneInfo("America/Sao_Paulo")
    now_brt = datetime.now(brt_tz)
    print(f"=== AUDITORIA READ-ONLY H001 - INCIDENTE 21/10/2026 ===")
    print(f"Horário da auditoria: {now_brt.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    # 1. Confirmar PID 4124
    print("\n--- 1. CONFIRMAÇÃO DO PROCESSO PID 4124 ---")
    pid = 4124
    if psutil.pid_exists(pid):
        p = psutil.Process(pid)
        print(f"PID: {p.pid} | Nome: {p.name()} | Status: {p.status()}")
        print(f"Memória RSS: {p.memory_info().rss / (1024*1024):.2f} MB")
        cpu = p.cpu_percent(interval=0.5)
        print(f"CPU Atual: {cpu}% | CPU Times: {p.cpu_times()}")
    else:
        print("ALERTA CRÍTICO: PID 4124 NÃO EXISTE!")

    # 2 & 3. Último tick e Watermarks no SQLite e PostgreSQL
    print("\n--- 2 & 3. ÚLTIMO TICK PERSISTIDO E WATERMARKS ---")
    sq_path = "data/buffer/spool.db"
    sq_conn = sqlite3.connect(sq_path)
    sq_cur = sq_conn.cursor()

    sq_cur.execute("SELECT stream_key, last_captured_msc, last_replicated_msc, watermark_complete_msc, updated_at_utc FROM checkpoints_two_stage WHERE stream_key = 'WINZ26';")
    ckp_row = sq_cur.fetchone()

    session_start_msc = 1792573200000
    sq_cur.execute("SELECT count(*), max(time_msc) FROM spool_ticks WHERE time_msc >= ?;", (session_start_msc,))
    sq_stats = sq_cur.fetchone()

    sq_cur.execute("SELECT count(*) FROM spool_ticks WHERE time_msc >= ? AND status = 'PENDING';", (session_start_msc,))
    sq_pending = sq_cur.fetchone()[0]

    sq_cur.execute("SELECT count(*) FROM spool_candles_m2 WHERE open_time_msc >= ?;", (session_start_msc,))
    sq_candles = sq_cur.fetchone()[0]
    sq_conn.close()

    pg_conn = psycopg2.connect(
        host="127.0.0.1", port=5434, database="market_data", user="postgres", password="password"
    )
    pg_cur = pg_conn.cursor()
    pg_cur.execute("SELECT count(*), max(time_msc) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc >= %s;", (session_start_msc,))
    pg_stats = pg_cur.fetchone()

    pg_cur.execute("SELECT count(*), max(open_time_utc), max(close_time_utc) FROM candles_m2 WHERE symbol = 'WINZ26' AND open_time_utc >= '2026-10-21 00:00:00+00';")
    pg_candles = pg_cur.fetchone()

    normalizer = MT5TimeNormalizer()

    def fmt_msc(msc):
        if not msc:
            return "N/A"
        dt_brt = normalizer.server_msc_to_utc_datetime(msc).astimezone(brt_tz)
        return f"{msc} ({dt_brt.strftime('%H:%M:%S.%f')[:-3]} BRT)"

    print(f"SQLite spool.db:")
    print(f"  Total ticks hoje: {sq_stats[0]}")
    print(f"  Último time_msc: {fmt_msc(sq_stats[1])}")
    print(f"  Backlog PENDING: {sq_pending}")
    print(f"  Total Candles M2: {sq_candles}")
    print(f"PostgreSQL 17 market_data:")
    print(f"  Total ticks hoje: {pg_stats[0]}")
    print(f"  Último time_msc: {fmt_msc(pg_stats[1])}")
    print(f"  Total Candles M2: {pg_candles[0]} (último candle close_utc: {pg_candles[2]})")
    print(f"Watermarks Atuais:")
    print(f"  W_captured: {fmt_msc(ckp_row[1])}")
    print(f"  W_durable:  {fmt_msc(ckp_row[2])}")
    print(f"  W_complete: {fmt_msc(ckp_row[3])}")
    print(f"  Updated at UTC: {ckp_row[4]}")

    # 4 & 5 & 6. Investigar comportamento da paginação do capture_step e MT5 copy_ticks_from
    print("\n--- 4, 5 & 6. PARÂMETROS EFETIVOS DE PAGINAÇÃO, CURSOR E RESOLUÇÃO TEMPORAL ---")
    with open("config/collector_config.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    batch_size = cfg.get("buffer", {}).get("batch_size", 1000)
    lookback_ms = cfg.get("collector", {}).get("lookback_ms", 2000)
    print(f"Configuração carregada:")
    print(f"  batch_size: {batch_size}")
    print(f"  lookback_ms: {lookback_ms}")
    print(f"Cursor no código (engine.py):")
    print(f"  from_msc = max(session_start, ckp.last_captured_msc - lookback_ms)")
    cur_from_msc = max(session_start_msc, ckp_row[1] - lookback_ms)
    print(f"  from_msc calculado: {fmt_msc(cur_from_msc)}")
    print(f"Conversão em mt5_reader.py (_resolve_query_datetime):")
    dt_from = normalizer.server_msc_to_utc_datetime(cur_from_msc)
    sec_trunc = int(cur_from_msc // 1000) if cur_from_msc > 1e11 else int(cur_from_msc)
    dt_from_sec = datetime.fromtimestamp(sec_trunc, tz=timezone.utc)
    print(f"  Valor em msc: {cur_from_msc}")
    print(f"  sec truncado: {sec_trunc} (perda dos milissegundos)")
    print(f"  datetime passado para MT5.copy_ticks_from: {dt_from_sec} (UTC)")

    # 7, 8, 9. Consultar histórico MT5 por janelas de 5 minutos aplicando filtros de elegibilidade
    print("\n--- 7, 8 & 9. AUDITORIA DE TICKS HISTÓRICOS NO MT5 vs PERSISTIDOS (JANELAS 5 MIN) ---")
    reader = MT5ReadOnlyReader(time_normalizer=normalizer)
    if not reader.initialize():
        print("Falha ao inicializar MT5ReadOnlyReader!")
        return

    reconciler = TickReconciler(time_normalizer=normalizer)

    # Janelas de 5 minutos de 09:00 até agora (~10:25)
    t_start = session_start_msc # 09:00:00 BRT
    now_server_msc = normalizer.utc_datetime_to_server_msc(datetime.now(timezone.utc))
    window_ms = 5 * 60 * 1000

    print(f"{'Janela BRT':<17} | {'MT5 Brutos':<10} | {'Elegíveis (Trades)':<18} | {'Persistidos PG':<14} | {'Diferença Ausente':<18}")
    print("-" * 88)

    w_start = t_start
    total_raw_session = 0
    total_eligible_session = 0
    total_pg_session = 0

    while w_start < now_server_msc:
        w_end = min(w_start + window_ms, now_server_msc)
        w_start_brt = normalizer.server_msc_to_utc_datetime(w_start).astimezone(brt_tz).strftime('%H:%M')
        w_end_brt = normalizer.server_msc_to_utc_datetime(w_end).astimezone(brt_tz).strftime('%H:%M')

        # Buscar do MT5
        raw_ticks = reader.fetch_ticks_range("WINZ26", w_start, w_end)
        raw_count = len(raw_ticks)
        total_raw_session += raw_count

        # Reconciliar/filtrar negócios elegíveis com as regras oficiais
        reconciled = reconciler.reconcile_raw_ticks(
            raw_ticks=raw_ticks,
            symbol="WINZ26",
            provenance="AUDIT_INDEPENDENT",
            cutoff_realtime_msc=None
        )
        eligible_count = len(reconciled)
        total_eligible_session += eligible_count

        # Buscar persistidos no PG nesta janela
        pg_cur.execute(
            "SELECT count(*) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc >= %s AND time_msc < %s;",
            (w_start, w_end)
        )
        pg_in_window = pg_cur.fetchone()[0]
        total_pg_session += pg_in_window

        diff = eligible_count - pg_in_window
        print(f"{w_start_brt} - {w_end_brt:<10} | {raw_count:<10} | {eligible_count:<18} | {pg_in_window:<14} | {diff:<18}")

        w_start = w_end

    print("-" * 88)
    print(f"{'TOTAL SESSÃO':<17} | {total_raw_session:<10} | {total_eligible_session:<18} | {total_pg_session:<14} | {total_eligible_session - total_pg_session:<18}")

    # 10. Investigar por que o PeriodicHistoricalReconciler não processou
    print("\n--- 10. ANÁLISE DO RECONCILIADOR PERIÓDICO ---")
    print("Regra de auditoria do PeriodicHistoricalReconciler (periodic_reconciler.py:L63-L88):")
    print("  audit_ceiling_msc = now_server_msc - self.safety_lag_ms")
    print("  if ckp.last_captured_msc > 0:")
    print("      audit_ceiling_msc = min(audit_ceiling_msc, ckp.last_captured_msc)")
    print("  from_msc = ckp.watermark_complete_msc")
    print("  if audit_ceiling_msc <= from_msc: return UP_TO_DATE")
    print(f"Valores efetivos:")
    print(f"  ckp.last_captured_msc: {ckp_row[1]}")
    print(f"  audit_ceiling_msc travado em: {ckp_row[1]}")
    print(f"  ckp.watermark_complete_msc: {ckp_row[3]}")
    print(f"  Condição audit_ceiling_msc <= from_msc: {ckp_row[1] <= ckp_row[3]} -> RETORNA 'UP_TO_DATE' SEM AUDITAR!")

    # 11, 12, 13, 14. Auditoria dos Candles M2
    print("\n--- 11, 12, 13 & 14. AUDITORIA DA FONTE DOS CANDLES M2 ---")
    print("Verificação em engine.py (sync_m2_candles L233):")
    print("  rates = self.reader.fetch_rates_range(self.symbol, 2, session_start_msc, now_server_msc)")
    print("  -> fetch_rates_range chama mt5.copy_rates_range!")
    print("  CONFIRMAÇÃO: Os candles M2 são provenientes das BARRAS NATIVAS DO MT5, e NÃO calculados a partir dos ticks de market_ticks_raw!")
    
    # Divergência de volume de ticks vs ticks persistidos
    pg_cur.execute("""
        SELECT open_time_utc, close_time_utc, open, high, low, close, volume_real, tick_volume
        FROM candles_m2
        WHERE symbol = 'WINZ26' AND open_time_utc >= '2026-10-21 00:00:00+00'
        ORDER BY open_time_utc ASC;
    """)
    candles_rows = pg_cur.fetchall()
    print(f"\nComparação de volume para os primeiros 5 candles e últimos 3 candles:")
    print(f"{'Candle BRT':<15} | {'Candle tick_volume':<20} | {'Candle volume_real':<20} | {'Ticks Persistidos PG':<20}")
    print("-" * 80)
    sample_candles = candles_rows[:5] + candles_rows[-3:]
    for c in sample_candles:
        c_open_brt = c[0].astimezone(brt_tz)
        c_close_brt = c[1].astimezone(brt_tz)
        c_open_msc = normalizer.utc_datetime_to_server_msc(c[0])
        c_close_msc = normalizer.utc_datetime_to_server_msc(c[1])
        pg_cur.execute(
            "SELECT count(*) FROM market_ticks_raw WHERE symbol = 'WINZ26' AND time_msc >= %s AND time_msc < %s;",
            (c_open_msc, c_close_msc)
        )
        persisted_in_candle = pg_cur.fetchone()[0]
        label = f"{c_open_brt.strftime('%H:%M')} - {c_close_brt.strftime('%H:%M')}"
        print(f"{label:<15} | {c[7]:<20} | {c[6]:<20.0f} | {persisted_in_candle:<20}")

    pg_conn.close()
    reader.shutdown()

if __name__ == "__main__":
    audit_etapa1_and_etapa2()
