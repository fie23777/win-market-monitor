import os
import sys
from datetime import datetime, timezone
import zoneinfo
import MetaTrader5 as mt5

from src.connection.mt5_reader import MT5ReadOnlyReader
from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.reconciliation.tick_reconciler import TickReconciler

def reproduce_pagination():
    print("=== ETAPA 3: REPRODUÇÃO ISOLADA DO PROBLEMA DE PAGINAÇÃO ===")
    normalizer = MT5TimeNormalizer()
    brt_tz = zoneinfo.ZoneInfo("America/Sao_Paulo")
    
    reader = MT5ReadOnlyReader(time_normalizer=normalizer)
    if not reader.initialize():
        print("Falha ao inicializar MT5!")
        return

    symbol = "WINZ26"
    # Ponto de travamento: 1792573258338 (09:00:58.338 BRT)
    stuck_msc = 1792573258338
    lookback_ms = 2000
    batch_size = 1000

    print(f"\n--- 15 & 16. REPRODUÇÃO DO LOOP INFINITO DE PAGINAÇÃO ---")
    from_msc = stuck_msc - lookback_ms # 1792573256338 (09:00:56.338 BRT)
    print(f"Cursor inicial de busca: {from_msc} ({normalizer.server_msc_to_utc_datetime(from_msc).astimezone(brt_tz).strftime('%H:%M:%S.%f')[:-3]} BRT)")

    # Simular 3 chamadas consecutivas como o engine faz
    reconciler = TickReconciler(time_normalizer=normalizer)
    
    for cycle in range(1, 4):
        raw_ticks = reader.fetch_ticks_from(symbol, from_msc, count=batch_size)
        first_t = raw_ticks[0]["time_msc"] if raw_ticks else 0
        last_t = raw_ticks[-1]["time_msc"] if raw_ticks else 0
        dt_first = normalizer.server_msc_to_utc_datetime(first_t).astimezone(brt_tz).strftime('%H:%M:%S.%f')[:-3]
        dt_last = normalizer.server_msc_to_utc_datetime(last_t).astimezone(brt_tz).strftime('%H:%M:%S.%f')[:-3]
        print(f"Ciclo {cycle}: retornou {len(raw_ticks)} ticks | Primeiro: {first_t} ({dt_first}) | Último: {last_t} ({dt_last})")
        
        reconciled = reconciler.reconcile_raw_ticks(raw_ticks, symbol, "REALTIME_STREAM", cutoff_realtime_msc=None)
        print(f"        Após reconciliação determinística: {len(reconciled)} novos ticks aceitos")
        if len(reconciled) == 0:
            print("        -> RECONCILIADOR IDENTIFICOU DUPLICATAS: 0 ticks novos! last_captured_msc NÃO AVANÇA!")

    # 16. Densidade de negócios no segundo 09:00:58
    print(f"\n--- 16. DENSIDADE NO SEGUNDO 09:00:58 (1792573258000 a 1792573259000) ---")
    sec_58_start = 1792573258000
    sec_58_end = 1792573259000
    sec_58_ticks = reader.fetch_ticks_range(symbol, sec_58_start, sec_58_end)
    print(f"Total de ticks no MT5 entre 09:00:58.000 e 09:00:59.000: {len(sec_58_ticks)} ticks!")
    
    # Contagem de ticks com o exato timestamp 1792573258338
    exact_same_msc = [t for t in sec_58_ticks if t["time_msc"] == stuck_msc]
    print(f"Total de ticks com o exato time_msc = {stuck_msc}: {len(exact_same_msc)}")

    # 17 & 18. Análise do perigo de adicionar +1 ms arbitrário
    print(f"\n--- 17 & 18. AVALIAÇÃO DE AVANÇO DE CURSOR: PERIGO DE '+1 ms' vs MULTIPLICIDADES ---")
    # Quantos ticks no lote de 1.000 terminaram em stuck_msc?
    ticks_at_boundary = [t for t in raw_ticks if t["time_msc"] == last_t]
    total_in_mt5_at_boundary = [t for t in sec_58_ticks if t["time_msc"] == last_t]
    print(f"No lote de 1.000 ticks, quantos tinham time_msc={last_t}: {len(ticks_at_boundary)}")
    print(f"No MT5 real, quantos existem com time_msc={last_t}: {len(total_in_mt5_at_boundary)}")
    if len(ticks_at_boundary) < len(total_in_mt5_at_boundary):
        print(f"ALERTA CRÍTICO: O batch de 1.000 foi cortado no meio dos ticks do milissegundo {last_t}!")
        print(f"  Truncar com cursor = {last_t} + 1 ms PERDERIA {len(total_in_mt5_at_boundary) - len(ticks_at_boundary)} negócios legítimos!")
    else:
        print(f"  O lote cobriu todos os ticks desse milissegundo exato.")

    # 19. Demonstração de Soluções Arquiteturais Seguras
    print(f"\n--- 19. TESTE DE PAGINAÇÃO DETERMINÍSTICA SEGURA ---")
    print("Abordagem A: copy_ticks_range em fatias temporais (range-based chunking)")
    # Testar se fetch_ticks_range consegue paginar sem travar
    slice_start = stuck_msc
    slice_end = slice_start + 10_000 # 10 segundos
    range_slice = reader.fetch_ticks_range(symbol, slice_start, slice_end)
    print(f"  fetch_ticks_range({slice_start}, {slice_end}): retornou {len(range_slice)} ticks!")
    if range_slice:
        print(f"  Primeiro: {range_slice[0]['time_msc']} | Último: {range_slice[-1]['time_msc']}")

    print("\nAbordagem B: batch_size maior na API copy_ticks_from")
    # Testar se copy_ticks_from com count=50000 consegue saltar o bloco de alta densidade
    large_batch = reader.fetch_ticks_from(symbol, from_msc, count=50000)
    print(f"  fetch_ticks_from(count=50000): retornou {len(large_batch)} ticks!")
    if large_batch:
        dt_large_last = normalizer.server_msc_to_utc_datetime(large_batch[-1]['time_msc']).astimezone(brt_tz)
        print(f"  Último tick no batch de 50k: {large_batch[-1]['time_msc']} ({dt_large_last.strftime('%H:%M:%S.%f')[:-3]} BRT)")

    reader.shutdown()

if __name__ == "__main__":
    reproduce_pagination()
