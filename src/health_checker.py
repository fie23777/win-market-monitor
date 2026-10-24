import os
import sys
import json
import socket
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta, time as dt_time
from typing import Dict, Any, Optional

try:
    import zoneinfo
    BRT_TZ = zoneinfo.ZoneInfo("America/Sao_Paulo")
except Exception:
    BRT_TZ = timezone(timedelta(hours=-3))

from src.config import load_config, AppConfig
from src.utils.process_lock import CollectorProcessLock
from src.buffer.durable_spool import DurableSpool
from src.connection.mt5_reader import MT5ReadOnlyReader

logger = logging.getLogger(__name__)

def is_b3_market_open(dt_now: Optional[datetime] = None) -> bool:
    """
    Determina se o mercado da B3 (mini-índice) está no horário regular de negociação.
    Dias úteis (Segunda a Sexta-feira, 0 a 4) entre 09:00 e 18:00 BRT.
    """
    now_utc = dt_now or datetime.now(timezone.utc)
    now_brt = now_utc.astimezone(BRT_TZ)
    weekday = now_brt.weekday()  # 0=Segunda, 6=Domingo
    
    if weekday >= 5:  # Sábado ou Domingo
        return False
        
    current_time = now_brt.time()
    market_open = dt_time(9, 0, 0)
    market_close = dt_time(18, 0, 0)
    
    return market_open <= current_time <= market_close

def check_tcp_port(host: str, port: int, timeout_sec: float = 2.0) -> bool:
    """Verifica se uma porta TCP está aberta e respondendo."""
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def check_docker_engine(timeout_sec: float = 3.0) -> Dict[str, Any]:
    """Verifica se o Docker Engine Linux está respondendo a comandos."""
    import subprocess
    try:
        proc = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_sec
        )
        if proc.returncode == 0:
            version = proc.stdout.strip()
            return {"available": True, "version": version, "message": f"Docker Engine disponível (v{version})."}
        else:
            err = proc.stderr.strip() or proc.stdout.strip()
            return {"available": False, "version": None, "message": f"Docker Engine indisponível: {err}"}
    except FileNotFoundError:
        return {"available": False, "version": None, "message": "Executável docker não encontrado no PATH."}
    except subprocess.TimeoutExpired:
        return {"available": False, "version": None, "message": "Timeout ao consultar docker info."}
    except Exception as e:
        return {"available": False, "version": None, "message": f"Erro ao consultar docker: {e}"}

def check_postgres_health(config: AppConfig, timeout_sec: float = 3.0) -> Dict[str, Any]:
    """Verifica saúde e conectividade real do PostgreSQL 17."""
    pg = config.postgres
    port_open = check_tcp_port(pg.host, pg.port, timeout_sec=1.5)
    if not port_open:
        return {
            "healthy": False,
            "port_open": False,
            "can_query": False,
            "message": f"Porta {pg.port} do PostgreSQL não está acessível em {pg.host}."
        }

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=pg.host,
            port=pg.port,
            dbname=pg.database,
            user=pg.user,
            password=pg.password,
            connect_timeout=int(timeout_sec)
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            res = cur.fetchone()
        conn.close()
        return {
            "healthy": bool(res and res[0] == 1),
            "port_open": True,
            "can_query": True,
            "message": f"PostgreSQL 17 saudável na porta {pg.port} (banco: {pg.database})."
        }
    except Exception as e:
        return {
            "healthy": False,
            "port_open": True,
            "can_query": False,
            "message": f"Falha de autenticação/consulta no PostgreSQL: {e}"
        }

def check_mt5_health(symbol: str, reader: Optional[MT5ReadOnlyReader] = None) -> Dict[str, Any]:
    """Verifica se o terminal MT5 está inicializado, conectado ao broker e com o símbolo selecionado."""
    r = reader or MT5ReadOnlyReader()
    try:
        init_ok = r.initialize()
        if not init_ok:
            return {
                "healthy": False,
                "connected": False,
                "symbol_available": False,
                "waiting_user": False,
                "message": "Terminal MT5 não inicializado ou fechado."
            }

        connected = r.is_connected()
        if not connected:
            r.shutdown()
            return {
                "healthy": False,
                "connected": False,
                "symbol_available": False,
                "waiting_user": True,
                "message": "Terminal MT5 aberto, porém desconectado da corretora (necessária intervenção humana)."
            }

        symbol_ok = r.select_symbol(symbol)
        r.shutdown()

        if not symbol_ok:
            return {
                "healthy": False,
                "connected": True,
                "symbol_available": False,
                "waiting_user": False,
                "message": f"Símbolo {symbol} não disponível ou não selecionável no Market Watch."
            }

        return {
            "healthy": True,
            "connected": True,
            "symbol_available": True,
            "waiting_user": False,
            "message": f"Terminal MT5 conectado à corretora com o símbolo {symbol} ativo."
        }
    except Exception as e:
        try:
            r.shutdown()
        except Exception:
            pass
        return {
            "healthy": False,
            "connected": False,
            "symbol_available": False,
            "waiting_user": False,
            "message": f"Erro ao verificar saúde do MT5: {e}"
        }

def evaluate_system_health(
    config: Optional[AppConfig] = None,
    max_pending_count: int = 5000,
    max_pending_age_sec: float = 60.0,
    max_heartbeat_lag_sec: float = 30.0,
    reader: Optional[MT5ReadOnlyReader] = None,
    spool: Optional[DurableSpool] = None
) -> Dict[str, Any]:
    """
    Avaliação de saúde ponta a ponta do WIN Market Monitor.
    Retorna diagnóstico estruturado com distinção formal entre:
    - HEALTHY
    - DEGRADED
    - WAITING_FOR_USER
    - UNHEALTHY
    """
    cfg = config or load_config()
    symbol = cfg.symbol
    lock_dir = Path(cfg.buffer.sqlite_path).parent
    sp = spool or DurableSpool(cfg.buffer.sqlite_path)

    now_utc = datetime.now(timezone.utc)
    now_brt = now_utc.astimezone(BRT_TZ)
    market_open = is_b3_market_open(now_utc)

    # 1. Infraestrutura: Docker Engine & PostgreSQL
    docker_diag = check_docker_engine()
    pg_diag = check_postgres_health(cfg)

    # 2. Conectividade MT5
    mt5_diag = check_mt5_health(symbol, reader=reader)

    # 3. Aplicação & Trava Atômica do Coletor
    lock_diag = CollectorProcessLock.inspect_lock_state(
        symbol=symbol,
        lock_dir=lock_dir,
        max_heartbeat_lag_sec=max_heartbeat_lag_sec
    )

    # 4. Spool SQLite WAL: Checkpoints, Alertas Operacionais e Pendências
    ckp = sp.get_checkpoints(symbol)
    alerts = sp.get_active_operational_alerts(symbol)
    inconclusives = sp.get_inconclusive_intervals(symbol, limit=10)
    pending_metrics = sp.get_pending_metrics(symbol)

    has_critical_stagnation = any(
        a.get("alert_type") == "CRITICAL_STAGNATION_LIMIT_EXCEEDED" for a in alerts
    )

    # Análise de pendências
    pending_count = pending_metrics["pending_ticks_count"]
    oldest_pending_msc = pending_metrics["oldest_pending_msc"]
    pending_age_sec = 0.0
    if oldest_pending_msc:
        oldest_pending_utc = sp.time_normalizer.server_msc_to_utc_datetime(oldest_pending_msc)
        pending_age_sec = max(0.0, (now_utc - oldest_pending_utc).total_seconds())

    # Determinação do Status Global
    status = "HEALTHY"
    reasons = []

    # Condições UNHEALTHY
    if has_critical_stagnation:
        status = "UNHEALTHY"
        reasons.append("Alerta crítico CRITICAL_STAGNATION_LIMIT_EXCEEDED persistido no buffer SQLite.")

    if lock_diag["status"] == "FROZEN_PROCESS":
        status = "UNHEALTHY"
        reasons.append(lock_diag["message"])

    if lock_diag["status"] in ("CORRUPTED_LOCK", "REUSED_PID"):
        status = "UNHEALTHY"
        reasons.append(lock_diag["message"])

    if ckp.last_captured_msc < 0 or ckp.last_replicated_msc < 0:
        status = "UNHEALTHY"
        reasons.append(f"Inconsistência crítica: checkpoints negativos (captura={ckp.last_captured_msc}, replicação={ckp.last_replicated_msc}).")

    if not docker_diag["available"]:
        status = "UNHEALTHY"
        reasons.append("Docker Engine indisponível.")

    if not pg_diag["healthy"]:
        # Se PG estiver indisponível mas coletor estiver rodando com buffer local,
        # pode ser DEGRADED caso esteja acumulando duravelmente
        if lock_diag["status"] == "ACTIVE_HEALTHY":
            if status != "UNHEALTHY":
                status = "DEGRADED"
            reasons.append(f"PostgreSQL inacessível ({pg_diag['message']}). Coletor retendo dados no buffer local WAL.")
        else:
            status = "UNHEALTHY"
            reasons.append(f"PostgreSQL indisponível ({pg_diag['message']}).")

    # Condições WAITING_FOR_USER
    if mt5_diag.get("waiting_user"):
        if status not in ("UNHEALTHY",):
            status = "WAITING_FOR_USER"
        reasons.append(mt5_diag["message"])

    elif not mt5_diag["healthy"]:
        if status != "UNHEALTHY":
            status = "DEGRADED" if not market_open else "UNHEALTHY"
        reasons.append(mt5_diag["message"])

    # Condições do Coletor
    if lock_diag["status"] == "NO_LOCK":
        if status not in ("UNHEALTHY", "WAITING_FOR_USER"):
            status = "DEGRADED" if not market_open else "UNHEALTHY"
        reasons.append("Processo coletor não está em execução.")

    elif lock_diag["status"] == "STALE_LOCK":
        if status not in ("UNHEALTHY", "WAITING_FOR_USER"):
            status = "DEGRADED"
        reasons.append(f"Lock obsoleto detectado (PID {lock_diag.get('pid')}).")

    # Condições de Pendências
    if pending_count > max_pending_count:
        if status == "HEALTHY":
            status = "DEGRADED"
        reasons.append(f"Pendências de replicação ({pending_count}) excederam o limite configurado ({max_pending_count}).")

    if market_open and oldest_pending_msc and pending_age_sec > max_pending_age_sec:
        if status == "HEALTHY":
            status = "DEGRADED"
        reasons.append(f"Atraso na replicação: pendência mais antiga tem {pending_age_sec:.1f}s (tolerância: {max_pending_age_sec:.1f}s).")

    # Se for fora do pregão e estiver tudo operacional
    market_note = "Horário de Negociação B3 (Mercado Aberto)" if market_open else "Fora do Horário de Pregão B3 (Standby)"

    return {
        "status": status,
        "symbol": symbol,
        "timestamp_brt": now_brt.strftime("%Y-%m-%d %H:%M:%S BRT"),
        "timestamp_utc": now_utc.isoformat(),
        "market_open": market_open,
        "market_note": market_note,
        "reasons": reasons,
        "infrastructure": {
            "docker": docker_diag,
            "postgres": pg_diag,
            "mt5": mt5_diag
        },
        "application": {
            "lock": lock_diag,
            "active_alerts_count": len(alerts),
            "critical_stagnation": has_critical_stagnation,
            "inconclusive_intervals_count": len(inconclusives)
        },
        "buffer": {
            "checkpoints": {
                "last_captured_msc": ckp.last_captured_msc,
                "last_replicated_msc": ckp.last_replicated_msc,
                "watermark_complete_msc": getattr(ckp, "watermark_complete_msc", 0)
            },
            "pending_metrics": {
                "pending_ticks_count": pending_count,
                "oldest_pending_age_sec": round(pending_age_sec, 2),
                "pending_candles_count": pending_metrics["pending_candles_count"]
            }
        }
    }

def print_health_report(diag: Dict[str, Any]) -> None:
    """Imprime relatório amigável em português no terminal."""
    status = diag["status"]
    symbol = diag["symbol"]
    time_brt = diag["timestamp_brt"]
    market_note = diag["market_note"]

    print("=================================================")
    print("  WIN MARKET MONITOR — VERIFICAÇÃO DE SAÚDE     ")
    print("=================================================")
    print(f"Horário de Referência: {time_brt}")
    print(f"Símbolo Monitorado:    {symbol}")
    print(f"Estado do Mercado:     {market_note}")
    print("-------------------------------------------------")

    # Infraestrutura
    docker = diag["infrastructure"]["docker"]
    if docker["available"]:
        print(f"[OK] Docker Engine: {docker['message']}")
    else:
        print(f"[FALHA] Docker Engine: {docker['message']}")

    pg = diag["infrastructure"]["postgres"]
    if pg["healthy"]:
        print(f"[OK] PostgreSQL: {pg['message']}")
    elif pg.get("port_open"):
        print(f"[AVISO] PostgreSQL: {pg['message']}")
    else:
        print(f"[FALHA] PostgreSQL: {pg['message']}")

    mt5 = diag["infrastructure"]["mt5"]
    if mt5["healthy"]:
        print(f"[OK] MetaTrader 5: {mt5['message']}")
    elif mt5.get("waiting_user"):
        print(f"[AGUARDANDO_USUÁRIO] MetaTrader 5: {mt5['message']}")
    else:
        print(f"[FALHA] MetaTrader 5: {mt5['message']}")

    # Aplicação
    lock = diag["application"]["lock"]
    if lock["status"] == "ACTIVE_HEALTHY":
        print(f"[OK] Coletor Python: {lock['message']}")
    elif lock["status"] == "FROZEN_PROCESS":
        print(f"[FALHA] Coletor Python: {lock['message']}")
    elif lock["status"] == "NO_LOCK":
        print("[AVISO] Coletor Python: Processo não iniciado.")
    else:
        print(f"[FALHA] Coletor Python: {lock['message']}")

    # Buffer & Checkpoints
    buf = diag["buffer"]
    ckp = buf["checkpoints"]
    pnd = buf["pending_metrics"]
    print(f"[INFO] Checkpoints: Capturado={ckp['last_captured_msc']} msc | Replicado={ckp['last_replicated_msc']} msc")
    print(f"[INFO] Buffer Local: Pendências={pnd['pending_ticks_count']} ticks (Idade mais antiga: {pnd['oldest_pending_age_sec']}s)")

    if diag["application"]["critical_stagnation"]:
        print("[FALHA CRÍTICA] Alerta de estagnação crítica ativo no SQLite spool!")

    print("-------------------------------------------------")
    print(f"STATUS OPERACIONAL: {status}")
    if diag["reasons"]:
        print("Motivos / Diagnóstico:")
        for r in diag["reasons"]:
            print(f" - {r}")
    print("=================================================")

def main():
    parser = argparse.ArgumentParser(description="Verificador de Saúde do WIN Market Monitor")
    parser.add_argument("--json", action="store_true", help="Emite resultado em formato JSON")
    parser.add_argument("--symbol", type=str, default=None, help="Símbolo para verificação (default: configurado)")
    args = parser.parse_args()

    cfg = load_config()
    if args.symbol:
        cfg.symbol = args.symbol.upper()

    diag = evaluate_system_health(config=cfg)

    if args.json:
        print(json.dumps(diag, indent=2, ensure_ascii=False))
    else:
        print_health_report(diag)

    # Código de saída padronizado:
    # 0 = HEALTHY
    # 1 = DEGRADED ou WAITING_FOR_USER
    # 2 = UNHEALTHY
    if diag["status"] == "HEALTHY":
        sys.exit(0)
    elif diag["status"] in ("DEGRADED", "WAITING_FOR_USER"):
        sys.exit(1)
    else:
        sys.exit(2)

if __name__ == "__main__":
    main()
