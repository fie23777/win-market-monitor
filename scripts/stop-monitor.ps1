<#
.SYNOPSIS
    Parada graciosa e segura do coletor WIN Market Monitor no Windows.
.DESCRIPTION
    Solicita o encerramento seguro do processo coletor via gatilho nao-destrutivo.
    Aguarda a finalizacao do ciclo de coleta e dreno final de pendencias.
    Preserva o buffer SQLite WAL e preserva os containers Docker e MT5 compartilhados.
.PARAMETER ConfigPath
    Caminho do arquivo de configuracao (default: config/startup_config.json).
.PARAMETER TimeoutSec
    Tempo maximo de espera em segundos para o encerramento gracioso (default: 25).
.PARAMETER Force
    Forca encerramento do processo caso o timeout seja atingido.
#>

[CmdletBinding()]
param (
    [string]$ConfigPath = "config/startup_config.json",
    [int]$TimeoutSec = 25,
    [switch]$Force
)

$ErrorActionPreference = "Continue"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location -Path $ProjectRoot

# Carregar configuracoes
$ResolvedConfig = Join-Path $ProjectRoot $ConfigPath
$Symbol = "WINZ26"
$WaitTimeout = $TimeoutSec

if (Test-Path $ResolvedConfig) {
    try {
        $Config = Get-Content -Path $ResolvedConfig -Raw | ConvertFrom-Json
        if ($Config.symbol) { $Symbol = $Config.symbol }
        if ($Config.graceful_stop_timeout_sec) { $WaitTimeout = [int]$Config.graceful_stop_timeout_sec }
    } catch {
        Write-Warning "Falha ao ler $ResolvedConfig. Usando padroes."
    }
}

$LockFile = Join-Path $ProjectRoot "data\buffer\collector_$Symbol.lock"
$ShutdownFile = Join-Path $ProjectRoot "data\buffer\shutdown.request"

Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  WIN MARKET MONITOR — ENCERRAMENTO SEGURO      " -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan

if (-not (Test-Path $LockFile)) {
    Write-Host "[INFO] Nenhum processo coletor ativo encontrado para o simbolo $Symbol." -ForegroundColor Yellow
    exit 0
}

# Ler metadados do lockfile
$LockData = $null
try {
    $LockData = Get-Content -Path $LockFile -Raw | ConvertFrom-Json
} catch {
    Write-Warning "Arquivo de trava encontrado, mas nao pode ser interpretado."
}

$TargetPid = if ($LockData -and $LockData.pid) { [int]$LockData.pid } else { $null }

if ($TargetPid) {
    Write-Host "[INFO] Solicitando parada graciosa para o coletor (PID $TargetPid)..." -ForegroundColor White
} else {
    Write-Host "[INFO] Solicitando parada graciosa para o coletor..." -ForegroundColor White
}

# Criar gatilho de shutdown gracioso
Set-Content -Path $ShutdownFile -Value "SHUTDOWN_REQUESTED $(Get-Date -Format o)" -Force

# Aguardar encerramento do processo e liberacao do lock
$Elapsed = 0
$ProcessStopped = $false

while ($Elapsed -lt $WaitTimeout) {
    Start-Sleep -Seconds 1
    $Elapsed++

    $ProcessRunning = $false
    if ($TargetPid) {
        $p = Get-Process -Id $TargetPid -ErrorAction SilentlyContinue
        if ($p -and -not $p.HasExited) {
            $ProcessRunning = $true
        }
    }

    if (-not (Test-Path $LockFile) -and -not $ProcessRunning) {
        $ProcessStopped = $true
        break
    }
    
    Write-Host -NoNewline "."
}
Write-Host ""

# Limpar arquivo de solicitacao de shutdown
if (Test-Path $ShutdownFile) {
    Remove-Item -Path $ShutdownFile -Force -ErrorAction SilentlyContinue
}

if ($ProcessStopped) {
    Write-Host "[OK] Coletor encerrado com seguranca. Buffer SQLite WAL e estado transacional integros." -ForegroundColor Green
    Write-Host "[INFO] Docker Desktop, PostgreSQL e MetaTrader 5 preservados para uso operacional." -ForegroundColor Cyan
    exit 0
} else {
    Write-Host "[AVISO] O coletor nao encerrou dentro do timeout de $WaitTimeout s." -ForegroundColor Yellow
    if ($Force -and $TargetPid) {
        Write-Host "[ATENCAO] Encerrando processo PID $TargetPid forcadamente conforme solicitado (-Force)..." -ForegroundColor Red
        Stop-Process -Id $TargetPid -Force -ErrorAction SilentlyContinue
        Start-Sleep -Seconds 1
        if (Test-Path $LockFile) {
            Remove-Item -Path $LockFile -Force -ErrorAction SilentlyContinue
        }
        Write-Host "[OK] Processo encerrado." -ForegroundColor Yellow
        exit 0
    } else {
        Write-Host "[ERRO] Processo ainda em execucao. Utilize -Force caso deseje termino imediato." -ForegroundColor Red
        exit 1
    }
}
