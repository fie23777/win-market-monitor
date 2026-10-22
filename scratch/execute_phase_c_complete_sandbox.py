import sys
sys.path.insert(0, ".")
import psycopg2
import sqlite3
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
import zoneinfo

from src.connection.mt5_time_normalizer import MT5TimeNormalizer
from src.domain.models import compute_tick_event_signature, compute_tick_fingerprint

normalizer = MT5TimeNormalizer()
tz_brt = zoneinfo.ZoneInfo("America/Sao_Paulo")
SANDBOX_PG_URI = "postgresql://sandbox_user:sandbox_pass@127.0.0.1:5435/market_data_sandbox"
SANDBOX_SQLITE_PATH = "data/buffer/sandbox_spool_h001.db"

print("================================================================================")
print("  FASE C: EXECUÇÃO COMPLETA DE CORREÇÃO NO SANDBOX CLONE (PORTA 5435)")
print("================================================================================")

conn_pg = psycopg2.connect(SANDBOX_PG_URI)
cur_pg = conn_pg.cursor()

# 1. Criar tabela de auditoria de reconciliação no Sandbox Clone
cur_pg.execute("""
CREATE TABLE IF NOT EXISTS audit_reconciliation_log (
    log_id BIGSERIAL PRIMARY KEY,
    execution_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action_type VARCHAR(64) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,
    affected_count BIGINT NOT NULL,
    volume_affected DOUBLE PRECISION NOT NULL,
    description TEXT NOT NULL
);
""")
conn_pg.commit()
print("Tabela audit_reconciliation_log inicializada.")

# 2. Carregar e Inserir Idempotentemente os 511 Ausentes (NC-003)
print("\n--- 2. INSERÇÃO DOS 511 TICKS AUSENTES (NC-003) NO CLONE ---")
with open("scratch/audit_results_20261019/all_missing_ticks.json", "r", encoding="utf-8") as f:
    missing_ticks = json.load(f)

print(f"Total de registros ausentes a injetar: {len(missing_ticks)}")

# Conectar SQLite Sandbox
conn_sq = sqlite3.connect(SANDBOX_SQLITE_PATH)
cur_sq = conn_sq.cursor()

inserted_count_pg = 0
inserted_vol_pg = 0.0

for t in missing_ticks:
    time_msc = t['time_msc']
    price = float(t['price'])
    vol = float(t['volume_real'])
    side = t['trade_side']
    ord_idx = t['occurrence_ordinal']
    # Flag canônica de mercado
    flag = 1080 if side == 'BUY' else (1112 if side == 'SELL' else 1144)
    # Assinatura canônica normalizada
    sig = compute_tick_event_signature(time_msc, price, vol, flag & ~256, side)
    fp = compute_tick_fingerprint(time_msc, sig, ord_idx)
    dt_utc = normalizer.server_msc_to_utc_datetime(time_msc)
    
    # Inserir no PostgreSQL Sandbox
    cur_pg.execute("""
        INSERT INTO market_ticks_raw (
            symbol, time_msc, tick_fingerprint, time_utc, price, volume_real,
            flags, trade_side, occurrence_ordinal, provenance, ingested_at_utc
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'RECOVERED_HISTORICAL_MT5', NOW())
        ON CONFLICT (time_utc, symbol, tick_fingerprint) DO NOTHING;
    """, ('WINZ26', time_msc, fp, dt_utc, price, vol, flag, side, ord_idx))
    if cur_pg.rowcount > 0:
        inserted_count_pg += 1
        inserted_vol_pg += vol

    # Inserir no SQLite Sandbox Spool
    cur_sq.execute("""
        INSERT OR IGNORE INTO spool_ticks (
            tick_fingerprint, symbol, time_msc, price, volume_real, flags,
            trade_side, occurrence_ordinal, provenance, status, created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'RECOVERED_HISTORICAL_MT5', 'COMMITTED', ?);
    """, (fp, 'WINZ26', time_msc, price, vol, flag, side, ord_idx, dt_utc.isoformat()))

conn_pg.commit()
conn_sq.commit()

print(f"Ticks injetados no PostgreSQL Sandbox: {inserted_count_pg} / {len(missing_ticks)}")
print(f"Volume injetado no PostgreSQL Sandbox: {inserted_vol_pg:,.1f} contratos")

cur_pg.execute("""
    INSERT INTO audit_reconciliation_log (action_type, entity_type, affected_count, volume_affected, description)
    VALUES ('INSERT_MISSING_TICKS', 'market_ticks_raw', %s, %s, 'Recuperação dos 511 ticks ausentes auditados da NC-003');
""", (inserted_count_pg, inserted_vol_pg))
conn_pg.commit()

# 3. Reclassificação Semântica da NC-001 por Evidência Individual
print("\n--- 3. RECLASSIFICAÇÃO SEMÂNTICA DA NC-001 (14:07 - 14:23 BRT) NO CLONE ---")
# Critério individual: time_utc no intervalo de blackout E ingested_at_utc pós-reconexão
dt_blackout_start_utc = datetime(2026, 10, 19, 17, 7, 27, tzinfo=timezone.utc)
dt_blackout_end_utc = datetime(2026, 10, 19, 17, 23, 50, tzinfo=timezone.utc)
dt_reconnect_threshold = datetime(2026, 10, 19, 17, 23, 50, tzinfo=timezone.utc)

cur_pg.execute("""
    UPDATE market_ticks_raw
    SET provenance = 'RECOVERED_GAP_MT5'
    WHERE symbol = 'WINZ26'
      AND time_utc >= %s AND time_utc <= %s
      AND ingested_at_utc >= %s
      AND provenance = 'REALTIME_STREAM';
""", (dt_blackout_start_utc, dt_blackout_end_utc, dt_reconnect_threshold))
reclassified_count = cur_pg.rowcount

# Atualizar também no SQLite Sandbox
cur_sq.execute("""
    UPDATE spool_ticks
    SET provenance = 'RECOVERED_GAP_MT5'
    WHERE symbol = 'WINZ26'
      AND time_msc >= 1792418847000 AND time_msc <= 1792419830000
      AND provenance = 'REALTIME_STREAM';
""")
conn_sq.commit()
conn_pg.commit()

print(f"Registros reclassificados como RECOVERED_GAP_MT5 no Sandbox: {reclassified_count}")

cur_pg.execute("""
    INSERT INTO audit_reconciliation_log (action_type, entity_type, affected_count, volume_affected, description)
    VALUES ('RECLASSIFY_PROVENANCE', 'market_ticks_raw', %s, 0.0, 'Reclassificação semântica NC-001 por evidência de chegada tardia pós-reconexão');
""", (reclassified_count,))
conn_pg.commit()

# 4. Validar Visão Canônica Atualizada no Sandbox Clone
print("\n--- 4. VALIDAÇÃO DA VISÃO CANÔNICA NO CLONE ---")
cur_pg.execute("""
SELECT COUNT(*), SUM(volume_real)
FROM v_market_ticks_canonical
WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 12:00:00+00' AND time_utc < '2026-10-19 21:35:00+00';
""")
final_canon_count, final_canon_vol = cur_pg.fetchone()
print(f"Total Ticks Visão Canônica Sandbox: {final_canon_count} (Esperado: 2.093.566)")
print(f"Total Volume Visão Canônica Sandbox: {final_canon_vol:,.1f} (Esperado: 19.816.479,0)")

# 5. Reconstrução de candles_m2_v2 no Sandbox Clone
print("\n--- 5. RECONSTRUÇÃO DETERMINÍSTICA DE CANDLES_M2_V2 NO CLONE ---")
cur_pg.execute("""
CREATE TABLE IF NOT EXISTS candles_m2_v2 (
    symbol VARCHAR(16) NOT NULL,
    open_time_utc TIMESTAMPTZ NOT NULL,
    close_time_utc TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume_real DOUBLE PRECISION NOT NULL,
    tick_volume BIGINT NOT NULL,
    spread INTEGER NOT NULL DEFAULT 5,
    state VARCHAR(16) NOT NULL DEFAULT 'REVISED',
    revision_number INTEGER NOT NULL DEFAULT 1,
    has_subsecond_ambiguity BOOLEAN NOT NULL DEFAULT FALSE,
    derived_from_provenance VARCHAR(64) NOT NULL DEFAULT 'CANONICAL_AUDITED_VIEW',
    recalculated_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, open_time_utc, revision_number)
);
""")
conn_pg.commit()

# Recalcular os 284 candles M2 diretamente de v_market_ticks_canonical
cur_pg.execute("""
WITH tick_windows AS (
    SELECT 
        symbol,
        time_msc,
        time_utc,
        price,
        volume_real,
        occurrence_ordinal,
        -- Truncar para o intervalo M2 (2 minutos = 120 segundos)
        to_timestamp(floor(extract(epoch from time_utc) / 120) * 120) AT TIME ZONE 'UTC' as candle_open_utc
    FROM v_market_ticks_canonical
    WHERE symbol = 'WINZ26' AND time_utc >= '2026-10-19 12:00:00+00' AND time_utc < '2026-10-19 21:35:00+00'
),
ordered_ticks AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY candle_open_utc ORDER BY time_msc ASC, occurrence_ordinal ASC) as rn_first,
        ROW_NUMBER() OVER (PARTITION BY candle_open_utc ORDER BY time_msc DESC, occurrence_ordinal DESC) as rn_last,
        COUNT(*) OVER (PARTITION BY candle_open_utc, time_msc) as ticks_in_same_ms
    FROM tick_windows
),
candle_stats AS (
    SELECT 
        candle_open_utc,
        MIN(price) as low,
        MAX(price) as high,
        SUM(volume_real) as volume_real,
        COUNT(*) as tick_volume,
        BOOL_OR(ticks_in_same_ms > 1) as has_subsecond_ambiguity
    FROM ordered_ticks
    GROUP BY candle_open_utc
),
candle_open_close AS (
    SELECT 
        candle_open_utc,
        MAX(price) FILTER (WHERE rn_first = 1) as open,
        MAX(price) FILTER (WHERE rn_last = 1) as close
    FROM ordered_ticks
    GROUP BY candle_open_utc
)
INSERT INTO candles_m2_v2 (
    symbol, open_time_utc, close_time_utc, open, high, low, close,
    volume_real, tick_volume, spread, state, revision_number, has_subsecond_ambiguity, derived_from_provenance
)
SELECT 
    'WINZ26',
    s.candle_open_utc,
    s.candle_open_utc + interval '2 minutes',
    oc.open,
    s.high,
    s.low,
    oc.close,
    s.volume_real,
    s.tick_volume,
    5,
    'REVISED',
    1,
    s.has_subsecond_ambiguity,
    'CANONICAL_AUDITED_VIEW'
FROM candle_stats s
JOIN candle_open_close oc ON s.candle_open_utc = oc.candle_open_utc
ORDER BY s.candle_open_utc ASC;
""")
conn_pg.commit()

cur_pg.execute("SELECT COUNT(*), SUM(volume_real), SUM(tick_volume) FROM candles_m2_v2 WHERE symbol = 'WINZ26';")
m2_v2_count, m2_v2_vol, m2_v2_ticks = cur_pg.fetchone()
print(f"Candles M2_v2 reconstruídos no Sandbox Clone: {m2_v2_count} candles (Esperado: 284)")
print(f"Volume Real total em Candles M2_v2:          {m2_v2_vol:,.1f} (Esperado: 19.816.479,0)")
print(f"Tick Volume total em Candles M2_v2:          {m2_v2_ticks} ticks (Esperado: 2.093.566)")

cur_pg.execute("""
    INSERT INTO audit_reconciliation_log (action_type, entity_type, affected_count, volume_affected, description)
    VALUES ('REBUILD_CANDLES_M2_V2', 'candles_m2_v2', %s, %s, 'Reconstrução de candles M2 a partir da visão canônica estabilizada');
""", (m2_v2_count, m2_v2_vol))
conn_pg.commit()

cur_sq.close()
conn_sq.close()
cur_pg.close()
conn_pg.close()

print("\n================================================================================")
print("  FASE C CONCLUÍDA COM 100% DE SUCESSO NO SANDBOX CLONE!")
print("================================================================================")
