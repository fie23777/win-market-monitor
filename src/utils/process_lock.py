import os
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)

BRT_TZ = timezone(timedelta(hours=-3))

class ProcessLockError(RuntimeError):
    """Exceção levantada quando a exclusão mútua impede a execução."""
    pass

class CollectorProcessLock:
    """
    Mecanismo de exclusão mútua atômico para o coletor WIN Market Monitor.
    Combina:
    1. Named Mutex nativo do Windows (kernel object) para garantia atômica contra race conditions.
    2. Bloqueio exclusivo de arquivo (msvcrt / fcntl) associado ao ciclo de vida do processo.
    3. Metadados de processo (PID, timestamps BRT/UTC, heartbeat e linha de comando) para inspeção.
    4. Detecção de PID reutilizado e recuperação segura de locks órfãos (stale locks).
    """

    LOCK_OFFSET = 10000

    def __init__(self, symbol: str, lock_dir: str | Path, mutex_prefix: Optional[str] = None):
        self.symbol = symbol.upper()
        self.lock_dir = Path(lock_dir)
        self.lock_dir.mkdir(parents=True, exist_ok=True)
        self.lock_file_path = self.lock_dir / f"collector_{self.symbol}.lock"
        self.shutdown_file_path = self.lock_dir / "shutdown.request"
        
        prefix = mutex_prefix or "WinMarketMonitor"
        self.mutex_name = f"Local\\{prefix}_{self.symbol}"
        
        self._mutex_handle = None
        self._file_handle = None
        self._is_locked = False
        self._last_heartbeat_time = 0.0

    def _create_windows_mutex(self) -> Tuple[bool, Any]:
        """Cria ou adquire Named Mutex via Win32 API."""
        if sys.platform != "win32":
            return True, None

        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.windll.kernel32
        ERROR_ALREADY_EXISTS = 183
        WAIT_OBJECT_0 = 0
        WAIT_ABANDONED = 0x00000080
        WAIT_TIMEOUT = 0x00000102

        kernel32.SetLastError(0)
        handle = kernel32.CreateMutexW(None, True, self.mutex_name)
        err = kernel32.GetLastError()

        if not handle:
            return False, None

        if err == ERROR_ALREADY_EXISTS:
            # O mutex já existe. Testar se o dono atual ainda o segura.
            wait_res = kernel32.WaitForSingleObject(handle, 0)
            if wait_res in (WAIT_OBJECT_0, WAIT_ABANDONED):
                # O processo anterior encerrou/abandonou o mutex; agora somos os donos
                return True, handle
            else:
                # O mutex está ativamente segurado por outro processo
                kernel32.CloseHandle(handle)
                return False, None

        return True, handle

    def _close_windows_mutex(self):
        """Libera e fecha o Named Mutex Win32."""
        if sys.platform == "win32" and self._mutex_handle:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.ReleaseMutex(self._mutex_handle)
            kernel32.CloseHandle(self._mutex_handle)
            self._mutex_handle = None

    def acquire(self) -> bool:
        """
        Adquire atomicamente a trava de exclusão mútua para o coletor.
        Lança ProcessLockError se outra instância válida estiver executando.
        """
        if self._is_locked:
            return True

        # 1. Adquirir Named Mutex atômico
        acquired_mutex, handle = self._create_windows_mutex()
        if not acquired_mutex:
            active_info = self.get_active_lock_info()
            pid_info = f" (PID {active_info.get('pid')})" if active_info else ""
            raise ProcessLockError(
                f"Exclusão mútua ativada: Já existe uma instância do coletor executando para {self.symbol}{pid_info}."
            )
        self._mutex_handle = handle

        # 2. Bloqueio exclusivo em nível de arquivo
        try:
            self._file_handle = open(self.lock_file_path, "a+b")
            if sys.platform == "win32":
                import msvcrt
                try:
                    self._file_handle.seek(self.LOCK_OFFSET)
                    msvcrt.locking(self._file_handle.fileno(), msvcrt.LK_NBLCK, 1)
                except (IOError, OSError) as e:
                    self._close_windows_mutex()
                    self._file_handle.close()
                    self._file_handle = None
                    raise ProcessLockError(f"Exclusão mútua ativada: Já existe uma instância do coletor executando para {self.symbol} (arquivo ocupado).")
            else:
                import fcntl
                try:
                    fcntl.flock(self._file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except (IOError, OSError) as e:
                    self._close_windows_mutex()
                    self._file_handle.close()
                    self._file_handle = None
                    raise ProcessLockError(f"Exclusão mútua ativada: Já existe uma instância do coletor executando para {self.symbol} (arquivo ocupado).")

            # 3. Gravar metadados com identificação do processo
            now_utc = datetime.now(timezone.utc)
            now_brt = now_utc.astimezone(BRT_TZ)
            proc_create_time = None
            if psutil:
                try:
                    proc_create_time = psutil.Process(os.getpid()).create_time()
                except Exception:
                    pass
            self._create_time = proc_create_time
            self._started_at_utc = now_utc.isoformat()
            self._started_at_brt = now_brt.strftime("%Y-%m-%d %H:%M:%S BRT")

            metadata = {
                "pid": os.getpid(),
                "create_time": proc_create_time,
                "symbol": self.symbol,
                "started_at_utc": self._started_at_utc,
                "started_at_brt": self._started_at_brt,
                "heartbeat_utc": now_utc.isoformat(),
                "heartbeat_timestamp": time.time(),
                "command_line": " ".join(sys.argv),
                "status": "RUNNING"
            }
            self._file_handle.seek(0)
            self._file_handle.write(json.dumps(metadata, indent=2).encode("utf-8"))
            self._file_handle.flush()
            self._last_heartbeat_time = time.time()
            self._is_locked = True
            logger.info(f"Trava atômica adquirida com sucesso para {self.symbol} (PID {os.getpid()}).")
            return True

        except ProcessLockError:
            raise
        except Exception as e:
            self._close_windows_mutex()
            if self._file_handle:
                try:
                    self._file_handle.close()
                except Exception:
                    pass
                self._file_handle = None
            raise ProcessLockError(f"Erro ao inicializar arquivo de trava: {e}")

    def update_heartbeat(self, force: bool = False, min_interval_sec: float = 2.0) -> None:
        """
        Atualiza o heartbeat da trava no disco para comprovar progresso ativo.
        Evita I/O excessivo respeitando o intervalo mínimo configurado.
        """
        if not self._is_locked or not self._file_handle:
            return

        now = time.time()
        if not force and (now - self._last_heartbeat_time) < min_interval_sec:
            return

        try:
            now_utc = datetime.now(timezone.utc)
            now_brt = now_utc.astimezone(BRT_TZ)
            metadata = {
                "pid": os.getpid(),
                "create_time": getattr(self, "_create_time", None),
                "symbol": self.symbol,
                "started_at_utc": getattr(self, "_started_at_utc", now_utc.isoformat()),
                "started_at_brt": getattr(self, "_started_at_brt", now_brt.strftime("%Y-%m-%d %H:%M:%S BRT")),
                "heartbeat_utc": now_utc.isoformat(),
                "heartbeat_timestamp": now,
                "command_line": " ".join(sys.argv),
                "status": "RUNNING"
            }
            self._file_handle.seek(0)
            self._file_handle.truncate(0)
            self._file_handle.write(json.dumps(metadata, indent=2).encode("utf-8"))
            self._file_handle.flush()
            self._last_heartbeat_time = now
        except Exception as e:
            logger.warning(f"Aviso: falha ao atualizar heartbeat do processo: {e}")

    def is_shutdown_requested(self) -> bool:
        """Verifica se há solicitação de encerramento gracioso via arquivo de controle."""
        return self.shutdown_file_path.exists()

    def clear_shutdown_request(self) -> None:
        """Remove o arquivo de solicitação de encerramento."""
        try:
            if self.shutdown_file_path.exists():
                self.shutdown_file_path.unlink()
        except Exception as e:
            logger.warning(f"Aviso ao remover arquivo de shutdown: {e}")

    def release(self) -> None:
        """Libera a trava atômica, o descritor de arquivo e o named mutex."""
        if not self._is_locked:
            return

        try:
            if self._file_handle:
                try:
                    if sys.platform == "win32":
                        import msvcrt
                        self._file_handle.seek(self.LOCK_OFFSET)
                        msvcrt.locking(self._file_handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(self._file_handle, fcntl.LOCK_UN)
                except Exception:
                    pass
                self._file_handle.close()
                self._file_handle = None

            if self.lock_file_path.exists():
                try:
                    self.lock_file_path.unlink()
                except Exception:
                    pass

            self._close_windows_mutex()
            self.clear_shutdown_request()
            self._is_locked = False
            logger.info(f"Trava atômica liberada com sucesso para {self.symbol}.")
        except Exception as e:
            logger.error(f"Erro ao liberar trava atômica: {e}")

    def get_active_lock_info(self) -> Optional[Dict[str, Any]]:
        """Lê os metadados da trava ativa caso o arquivo exista."""
        if not self.lock_file_path.exists():
            return None
        try:
            with open(self.lock_file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data
        except Exception:
            return None

    @classmethod
    def inspect_lock_state(
        cls,
        symbol: str,
        lock_dir: str | Path,
        max_heartbeat_lag_sec: float = 30.0
    ) -> Dict[str, Any]:
        """
        Inspeciona o estado da trava sem tentar adquirir propriedade destrutiva.
        Retorna diagnóstico completo sobre vida do processo, concorrência e integridade.
        """
        symbol = symbol.upper()
        lock_file = Path(lock_dir) / f"collector_{symbol}.lock"

        if not lock_file.exists():
            return {"status": "NO_LOCK", "message": "Nenhuma trava encontrada no disco."}

        try:
            with open(lock_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            return {"status": "CORRUPTED_LOCK", "message": f"Arquivo de trava corrompido: {e}"}

        pid = data.get("pid")
        if not pid or not isinstance(pid, int):
            return {"status": "CORRUPTED_LOCK", "message": "PID ausente ou inválido no lockfile."}

        # Verificar se o processo com esse PID existe no SO
        pid_exists = False
        is_our_collector = False
        proc_name = ""

        if psutil:
            try:
                proc = psutil.Process(pid)
                pid_exists = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
                proc_name = proc.name()
                cmdline = " ".join(proc.cmdline())

                expected_ct = data.get("create_time")
                if expected_ct is not None:
                    actual_ct = proc.create_time()
                    # Se o tempo de criação diverge mais de 2.0s, o PID foi reutilizado por outro processo
                    if abs(actual_ct - expected_ct) > 2.0:
                        is_our_collector = False
                    else:
                        # Mesmo processo: verificar se é compatível com Python
                        is_python = "python" in proc_name.lower() or any("python" in a.lower() for a in proc.cmdline())
                        is_our_collector = is_python
                else:
                    # Fallback quando lockfile não tem create_time gravado
                    is_python = "python" in proc_name.lower() or any("python" in a.lower() for a in proc.cmdline())
                    is_cmd_match = any(k in cmdline.lower() for k in ("src.main", "win-market-monitor", "main.py", "pytest", "test_"))
                    is_our_collector = is_python and is_cmd_match
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pid_exists = False
        else:
            # Fallback sem psutil
            try:
                os.kill(pid, 0)
                pid_exists = True
                is_our_collector = True
            except (OSError, ProcessLookupError):
                pid_exists = False

        if not pid_exists:
            return {
                "status": "STALE_LOCK",
                "pid": pid,
                "message": f"Lock obsoleto: processo com PID {pid} não existe mais no sistema operacional.",
                "data": data
            }

        if not is_our_collector:
            return {
                "status": "REUSED_PID",
                "pid": pid,
                "message": f"PID reaproveitado: processo PID {pid} ({proc_name}) não corresponde ao coletor WIN Market Monitor.",
                "data": data
            }

        # Verificar heartbeat
        last_hb = data.get("heartbeat_timestamp", 0)
        now = time.time()
        hb_age = now - last_hb

        if hb_age > max_heartbeat_lag_sec:
            return {
                "status": "FROZEN_PROCESS",
                "pid": pid,
                "heartbeat_age_sec": round(hb_age, 2),
                "message": f"Processo coletor (PID {pid}) ativo, porém heartbeat estagnado há {hb_age:.1f}s (limite: {max_heartbeat_lag_sec}s).",
                "data": data
            }

        return {
            "status": "ACTIVE_HEALTHY",
            "pid": pid,
            "heartbeat_age_sec": round(hb_age, 2),
            "message": f"Coletor executando saudavelmente (PID {pid}, heartbeat há {hb_age:.1f}s).",
            "data": data
        }
