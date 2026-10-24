<#
.SYNOPSIS
    Reinicialização controlada do WIN Market Monitor.
.DESCRIPTION
    Executa a parada segura do processo coletor (dreno final e liberação de trava)
    e em seguida orquestra a inicialização completa com verificação de saúde.
.PARAMETER ConfigPath
    Caminho do arquivo de configuração (default: config/startup_config.json).
#>

[CmdletBinding()]
param (
    [string]$ConfigPath = "config/startup_config.json"
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "=================================================" -ForegroundColor Cyan
Write-Host "  WIN MARKET MONITOR — REINICIALIZAÇÃO CONTROLADA " -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan

# 1. Parada Segura
$StopScript = Join-Path $ScriptDir "stop-monitor.ps1"
& $StopScript -ConfigPath $ConfigPath
Start-Sleep -Seconds 2

# 2. Inicialização Automatizada
$StartScript = Join-Path $ScriptDir "start-monitor.ps1"
& $StartScript -ConfigPath $ConfigPath
$ExitCode = $LASTEXITCODE
exit $ExitCode
