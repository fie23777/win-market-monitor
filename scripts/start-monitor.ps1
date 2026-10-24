<#
.SYNOPSIS
    Inicializacao automatizada e orquestracao do WIN Market Monitor.
.DESCRIPTION
    Detecta o estado de todos os componentes (Docker Desktop, PostgreSQL, MetaTrader 5, Coletor Python),
    inicia apenas os componentes desligados, aguarda disponibilidade com timeouts controlados,
    impede instancias concorrentes com trava atomica e executa verificacao de integridade ponta a ponta.
.PARAMETER ConfigPath
    Caminho do arquivo de configuracao (default: config/startup_config.json).
.PARAMETER SkipHealthCheck
    Pula a verificacao final de saude caso solicitado.
#>

[CmdletBinding()]
param (
    [string]$ConfigPath = "config/startup_config.json",
    [switch]$SkipHealthCheck
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location -Path $ProjectRoot

# Carregar arquivo de configuracao externa
$ResolvedConfig = Join-Path $ProjectRoot $ConfigPath
$Config = @{
    symbol = "WINZ26"
    docker_desktop_path = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    mt5_terminal_path = "C:\Program Files\MetaTrader 5 Terminal\terminal64.exe"
    compose_file = "docker/docker-compose.yml"
    docker_timeout_sec = 120
    docker_retry_interval_sec = 3
    postgres_timeout_sec = 45
    postgres_retry_interval_sec = 2
    mt5_timeout_sec = 60
    log_dir = "data/logs"
    python_executable = "python"
}

if (Test-Path $ResolvedConfig) {
    try {
        $LoadedJson = Get-Content -Path $ResolvedConfig -Raw | ConvertFrom-Json
        foreach ($prop in $LoadedJson.PSObject.Properties) {
            $Config[$prop.Name] = $prop.Value
        }
    } catch {
        Write-Warning "Falha ao carregar $ResolvedConfig. Usando configuracoes padrao."
    }
}

$Symbol = $Config.symbol
$PythonExe = $Config.python_executable
$ComposeFile = $Config.compose_file

# Garantir existencia de diretorios essenciais
$LogDir = Join-Path $ProjectRoot $Config.log_dir
$BufferDir = Join-Path $ProjectRoot "data\buffer"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }
if (-not (Test-Path $BufferDir)) { New-Item -ItemType Directory -Path $BufferDir -Force | Out-Null }

Write-Host "=================================================" -ForegroundColor Cyan
Write-Host " WIN MARKET MONITOR — INICIALIZACAO AUTOMATICA   " -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "Horario Local: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') BRT" -ForegroundColor Gray
Write-Host "Ativo Alvo:    $Symbol" -ForegroundColor Gray
Write-Host "-------------------------------------------------" -ForegroundColor Gray

# -------------------------------------------------------------
# ETAPA 1: Pre-Requisitos do Host
# -------------------------------------------------------------
Write-Host "[1/6] Verificando pre-requisitos do ambiente Windows..." -ForegroundColor White

# 1.1 Verificar Python
try {
    $PyVersion = & $PythonExe --version 2>&1
    Write-Host " [OK] Python disponivel: $PyVersion" -ForegroundColor Green
} catch {
    Write-Host " [FALHA] Python nao encontrado no PATH ou nao acessivel." -ForegroundColor Red
    Write-Host " Instrucao: Instale o Python 3.12+ ou configure o caminho no config/startup_config.json." -ForegroundColor Yellow
    exit 1
}

# 1.2 Verificar Docker CLI
try {
    $DockerCli = & docker --version 2>&1
    Write-Host " [OK] Docker CLI disponivel: $DockerCli" -ForegroundColor Green
} catch {
    Write-Host " [FALHA] Comando 'docker' nao encontrado no PATH." -ForegroundColor Red
    Write-Host " Instrucao: Instale o Docker Desktop ou adicione ao PATH." -ForegroundColor Yellow
    exit 1
}

# 1.3 Verificar se o terminal MT5 existe no disco
$Mt5Exe = $Config.mt5_terminal_path
if (-not (Test-Path $Mt5Exe)) {
    $AltMt5 = "C:\Program Files\MetaTrader 5 Terminal\terminal64.exe"
    if (Test-Path $AltMt5) {
        $Mt5Exe = $AltMt5
    }
}
if (Test-Path $Mt5Exe) {
    Write-Host " [OK] Executavel MetaTrader 5 localizado: $Mt5Exe" -ForegroundColor Green
} else {
    Write-Host " [AVISO] Executavel do MT5 nao encontrado em $Mt5Exe." -ForegroundColor Yellow
}

# -------------------------------------------------------------
# ETAPA 2: Docker Desktop & Engine
# -------------------------------------------------------------
Write-Host "[2/6] Verificando Docker Desktop e Docker Engine..." -ForegroundColor White

function Test-DockerEngine {
    $info = & docker info --format "{{.ServerVersion}}" 2>&1
    return ($LASTEXITCODE -eq 0 -and $info -and -not ($info -like "*failed to connect*"))
}

$DockerOk = Test-DockerEngine
if (-not $DockerOk) {
    Write-Host " [INFO] Docker Engine nao esta respondendo. Verificando Docker Desktop..." -ForegroundColor Yellow
    $DockerDesktopExe = $Config.docker_desktop_path
    if (-not (Test-Path $DockerDesktopExe)) {
        Write-Host " [FALHA] Docker Desktop.exe nao encontrado em $DockerDesktopExe." -ForegroundColor Red
        exit 1
    }

    Write-Host " [INFO] Iniciando Docker Desktop ($DockerDesktopExe)..." -ForegroundColor White
    Start-Process -FilePath $DockerDesktopExe

    $DockerTimeout = [int]$Config.docker_timeout_sec
    $DockerElapsed = 0
    $DockerInterval = [int]$Config.docker_retry_interval_sec

    Write-Host " [INFO] Aguardando inicializacao do Docker Engine Linux (timeout: $DockerTimeout s)..." -ForegroundColor White
    while ($DockerElapsed -lt $DockerTimeout) {
        Start-Sleep -Seconds $DockerInterval
        $DockerElapsed += $DockerInterval
        if (Test-DockerEngine) {
            $DockerOk = $true
            break
        }
        Write-Host -NoNewline "."
    }
    Write-Host ""

    if (-not $DockerOk) {
        Write-Host " [FALHA] Timeout aguardando disponibilidade do Docker Engine." -ForegroundColor Red
        Write-Host " Diagnostico: Verifique se o Docker Desktop esta travado ou se o WSL2 precisa de reinicializacao." -ForegroundColor Yellow
        exit 1
    }
}

Write-Host " [OK] Docker Engine operacional e respondendo." -ForegroundColor Green

# -------------------------------------------------------------
# ETAPA 3: PostgreSQL Container via Docker Compose
# -------------------------------------------------------------
Write-Host "[3/6] Verificando container PostgreSQL (porta 5434)..." -ForegroundColor White

$ComposePath = Join-Path $ProjectRoot $ComposeFile
if (-not (Test-Path $ComposePath)) {
    Write-Host " [FALHA] Arquivo Docker Compose nao encontrado em $ComposePath." -ForegroundColor Red
    exit 1
}

# Iniciar PostgreSQL de forma idempotente
Write-Host " [INFO] Garantindo container PostgreSQL ativo via docker compose..." -ForegroundColor White
& docker compose -f $ComposePath up -d postgres | Out-Null

$PgTimeout = [int]$Config.postgres_timeout_sec
$PgElapsed = 0
$PgInterval = [int]$Config.postgres_retry_interval_sec
$PgReady = $false

while ($PgElapsed -lt $PgTimeout) {
    $inspectRes = & docker compose -f $ComposePath ps postgres --format json 2>&1
    if ($inspectRes -like "*running*" -or $inspectRes -like "*healthy*") {
        try {
            $tcp = New-Object System.Net.Sockets.TcpClient
            $connect = $tcp.BeginConnect("127.0.0.1", 5434, $null, $null)
            $wait = $connect.AsyncWaitHandle.WaitOne(1000, $false)
            if ($wait -and $tcp.Connected) {
                $tcp.EndConnect($connect)
                $tcp.Close()
                $PgReady = $true
                break
            }
            $tcp.Close()
        } catch {
            # Continuar tentando
        }
    }
    Start-Sleep -Seconds $PgInterval
    $PgElapsed += $PgInterval
    Write-Host -NoNewline "."
}
Write-Host ""

if (-not $PgReady) {
    Write-Host " [FALHA] PostgreSQL nao ficou disponivel na porta 5434 dentro do tempo limite." -ForegroundColor Red
    exit 1
}

Write-Host " [OK] PostgreSQL saudavel e respondendo na porta 5434." -ForegroundColor Green

# -------------------------------------------------------------
# ETAPA 4: MetaTrader 5 Terminal
# -------------------------------------------------------------
Write-Host "[4/6] Verificando MetaTrader 5 e conexao da corretora..." -ForegroundColor White

$Mt5Running = Get-Process -Name "*terminal64*" -ErrorAction SilentlyContinue
if (-not $Mt5Running -and (Test-Path $Mt5Exe)) {
    Write-Host " [INFO] Terminal MT5 nao esta aberto. Iniciando $Mt5Exe..." -ForegroundColor White
    Start-Process -FilePath $Mt5Exe
    Start-Sleep -Seconds 3
}

$Mt5Code = 'import sys; from src.connection.mt5_reader import MT5ReadOnlyReader; r = MT5ReadOnlyReader(); ok = (r.initialize() and r.is_connected() and r.select_symbol("' + $Symbol + '")); sys.exit(0 if ok else (11 if not r.is_connected() else 12))'
& $PythonExe -c $Mt5Code 2>&1 | Out-Null
$Mt5Exit = $LASTEXITCODE

if ($Mt5Exit -eq 0) {
    Write-Host " [OK] MetaTrader 5 conectado a corretora com o ativo $Symbol selecionado." -ForegroundColor Green
} elseif ($Mt5Exit -eq 11) {
    Write-Host " [AGUARDANDO_USUARIO] Terminal MT5 aberto, porem desconectado do servidor da corretora." -ForegroundColor Yellow
    Write-Host " Instrucao: Autentique sua conta no MetaTrader 5 antes de iniciar a captura de mercado." -ForegroundColor Yellow
} elseif ($Mt5Exit -eq 12) {
    Write-Host " [FALHA] Simbolo $Symbol nao pode ser selecionado no Market Watch do MT5." -ForegroundColor Red
} else {
    Write-Host " [AVISO] MT5 nao pode ser validado automaticamente (codigo: $Mt5Exit)." -ForegroundColor Yellow
}

# -------------------------------------------------------------
# ETAPA 5: Coletor Python & Exclusao Mutua Atomica
# -------------------------------------------------------------
Write-Host "[5/6] Verificando instancia do Coletor Python (exclusao mutua)..." -ForegroundColor White

$LockCode = 'import json; from src.utils.process_lock import CollectorProcessLock; print(json.dumps(CollectorProcessLock.inspect_lock_state("' + $Symbol + '", "data/buffer")))'
$LockJsonRaw = & $PythonExe -c $LockCode 2>&1
$LockDiag = $null
try {
    $LockDiag = $LockJsonRaw | ConvertFrom-Json
} catch {
    Write-Warning "Falha ao verificar trava atomica: $LockJsonRaw"
}

$NeedStartCollector = $true

if ($LockDiag) {
    if ($LockDiag.status -eq "ACTIVE_HEALTHY") {
        Write-Host " [OK] Coletor ja esta em execucao (PID $($LockDiag.pid)) e operando saudavelmente." -ForegroundColor Green
        Write-Host " [INFO] Nenhuma instancia duplicada foi criada (idempotencia preservada)." -ForegroundColor Cyan
        $NeedStartCollector = $false
    } elseif ($LockDiag.status -eq "FROZEN_PROCESS") {
        Write-Host (" [FALHA CRITICA] Processo coletor (PID " + $LockDiag.pid + ") esta com heartbeat estagnado (" + $LockDiag.heartbeat_age_sec + "s).") -ForegroundColor Red
        Write-Host " Diagnostico: O processo pode estar bloqueado em operacao do MT5/SQLite ou travado." -ForegroundColor Yellow
        Write-Host " Seguranca: Encerramento automatico forcado bloqueado para evitar perda de estado ou mascaramento de causa raiz." -ForegroundColor Yellow
        Write-Host " Instrucao: Investigue o processo ou execute stop-monitor.bat deliberadamente antes de reiniciar." -ForegroundColor Yellow
        exit 1
    } elseif ($LockDiag.status -in @("STALE_LOCK", "REUSED_PID", "CORRUPTED_LOCK")) {
        Write-Host " [INFO] Limpando trava orfa ou PID reaproveitado ($($LockDiag.status))..." -ForegroundColor Yellow
        $LockFile = Join-Path $ProjectRoot "data\buffer\collector_$Symbol.lock"
        if (Test-Path $LockFile) { Remove-Item -Path $LockFile -Force -ErrorAction SilentlyContinue }
        $NeedStartCollector = $true
    }
}

if ($NeedStartCollector) {
    $Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $CollectorLogFile = Join-Path $LogDir "collector_$Timestamp.log"

    Write-Host (" [INFO] Inicializando coletor em background: " + $PythonExe + " -m src.main") -ForegroundColor White
    Write-Host " [INFO] Logs da execucao: $CollectorLogFile" -ForegroundColor Gray

    $ProcInfo = New-Object System.Diagnostics.ProcessStartInfo
    $ProcInfo.FileName = $PythonExe
    $ProcInfo.Arguments = "-m src.main"
    $ProcInfo.WorkingDirectory = $ProjectRoot
    $ProcInfo.RedirectStandardOutput = $true
    $ProcInfo.RedirectStandardError = $true
    $ProcInfo.UseShellExecute = $false
    $ProcInfo.CreateNoWindow = $true

    $Proc = New-Object System.Diagnostics.Process
    $Proc.StartInfo = $ProcInfo
    
    $LogWriter = [System.IO.File]::AppendText($CollectorLogFile)
    $LogWriter.AutoFlush = $true

    $Proc.add_OutputDataReceived({
        param($sender, $e)
        if ($e.Data) { $LogWriter.WriteLine("[STDOUT] " + $e.Data) }
    })
    $Proc.add_ErrorDataReceived({
        param($sender, $e)
        if ($e.Data) { $LogWriter.WriteLine("[STDERR] " + $e.Data) }
    })

    $Proc.Start() | Out-Null
    $Proc.BeginOutputReadLine()
    $Proc.BeginErrorReadLine()

    # Aguardar 3 segundos para validacao da aquisicao da trava
    Start-Sleep -Seconds 3

    if ($Proc.HasExited) {
        Write-Host " [FALHA] O coletor encerrou imediatamente com codigo $($Proc.ExitCode)." -ForegroundColor Red
        Write-Host " Consulte os logs em: $CollectorLogFile" -ForegroundColor Yellow
        exit 1
    } else {
        Write-Host " [OK] Coletor inicializado com sucesso (PID $($Proc.Id))." -ForegroundColor Green
    }
}

# -------------------------------------------------------------
# ETAPA 6: Verificacao de Saude Ponta a Ponta
# -------------------------------------------------------------
if (-not $SkipHealthCheck) {
    Write-Host "-------------------------------------------------" -ForegroundColor Gray
    Write-Host "[6/6] Executando verificacao de saude integrada..." -ForegroundColor White
    Start-Sleep -Seconds 2
    
    $HealthScript = Join-Path $ScriptDir "check-monitor-health.ps1"
    & $HealthScript -ConfigPath $ConfigPath
    $FinalExit = $LASTEXITCODE
    exit $FinalExit
} else {
    Write-Host "[OK] Inicializacao concluida. Healthcheck ignorado conforme parametro -SkipHealthCheck." -ForegroundColor Green
    exit 0
}
