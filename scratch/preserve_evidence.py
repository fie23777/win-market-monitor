import os
import shutil
import sqlite3
import psycopg2
import psutil
from datetime import datetime, timezone
import zoneinfo

def main():
    brt_tz = zoneinfo.ZoneInfo("America/Sao_Paulo")
    now = datetime.now(brt_tz)
    print(f"=== FASE A: CONTENÇÃO E PRESERVAÇÃO DE EVIDÊNCIAS ({now.strftime('%Y-%m-%d %H:%M:%S %Z')}) ===")

    # 1. Processo PID 4124
    if psutil.pid_exists(4124):
        p = psutil.Process(4124)
        print(f"PID 4124: Ativo | Status: {p.status()} | RSS: {p.memory_info().rss / (1024*1024):.2f} MB")
    else:
        print("ALERTA: PID 4124 NÃO EXISTE!")

    # 2. Criar diretório de preservação de evidências
    evid_dir = "data/backup/incident_h001_20261021"
    os.makedirs(evid_dir, exist_ok=True)

    # 3. Copiar log do coletor
    log_src = r"C:\Users\fie23\.gemini\antigravity-ide\brain\a895cde0-b5b5-47d4-bdcc-18b51fec3fba\.system_generated\tasks\task-71.log"
    if os.path.exists(log_src):
        dst = os.path.join(evid_dir, "collector_task_71_evidence.log")
        shutil.copy2(log_src, dst)
        print(f"Log do coletor preservado em: {dst}")

    # 4. Snapshot dos checkpoints no SQLite
    sq_conn = sqlite3.connect("data/buffer/spool.db")
    c = sq_conn.cursor()
    c.execute("SELECT * FROM checkpoints_two_stage;")
    ckps = c.fetchall()
    sq_conn.close()

    ckpt_file = os.path.join(evid_dir, "checkpoints_snapshot.txt")
    with open(ckpt_file, "w") as f:
        f.write(f"Snapshot gerado em: {now.isoformat()}\n")
        f.write(f"Checkpoints: {ckps}\n")
    print(f"Checkpoints preservados em: {ckpt_file} -> {ckps}")

    # 5. Disponibilidade e volume do histórico MT5 atual
    from src.connection.mt5_reader import MT5ReadOnlyReader
    from src.connection.mt5_time_normalizer import MT5TimeNormalizer
    reader = MT5ReadOnlyReader()
    if reader.initialize():
        normalizer = MT5TimeNormalizer()
        session_start = 1792573200000
        now_msc = normalizer.utc_datetime_to_server_msc(datetime.now(timezone.utc))
        raw_ticks = reader.fetch_ticks_range("WINZ26", session_start, now_msc)
        rates = reader.fetch_rates_range("WINZ26", 2, session_start, now_msc)
        print(f"Histórico MT5 Disponível: {len(raw_ticks)} ticks brutos | {len(rates)} candles M2 nativos")
        reader.shutdown()

        with open(os.path.join(evid_dir, "mt5_availability_check.txt"), "w") as f:
            f.write(f"Timestamp: {now.isoformat()}\n")
            f.write(f"Total Ticks MT5 range [09:00, {now.strftime('%H:%M:%S')}]: {len(raw_ticks)}\n")
            f.write(f"Total Candles M2 nativos: {len(rates)}\n")

    print("Contenção e preservação concluídas com sucesso.")

if __name__ == "__main__":
    main()
