<#
.SYNOPSIS
    Verificação de saúde ponta a ponta do WIN Market Monitor.
.DESCRIPTION
    Avalia integridade da infraestrutura (Docker, PostgreSQL, MT5),
    da aplicação (Processo coletor, Trava atômica, Heartbeat),
    e dos dados (Checkpoints, Spool SQLite WAL, Pendências, Horário de Pregão).
.PARAMETER ConfigPath
    Caminho do arquivo de configuração (default: config/startup_config.json).
.PARAMETER Json
    Retorna a saída formatada em JSON estruturado.
.PARAMETER Symbol
    Símbolo a ser verificado (default: lê do config).
#>

[CmdletBinding()]
param (
    [string]$ConfigPath = "config/startup_config.json",
    [switch]$Json,
    [string]$Symbol = ""
)

$ErrorActionPreference = "Continue"

# Determinar diretório raiz do projeto
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location -Path $ProjectRoot

# Carregar configuração
$ResolvedConfigPath = Join-Path $ProjectRoot $ConfigPath
$PythonCmd = "python"
$TargetSymbol = $Symbol

if (Test-Path $ResolvedConfigPath) {
    try {
        $ConfigContent = Get-Content -Path $ResolvedConfigPath -Raw | ConvertFrom-Json
        if ($ConfigContent.python_executable) {
            $PythonCmd = $ConfigContent.python_executable
        }
        if (-not $TargetSymbol -and $ConfigContent.symbol) {
            $TargetSymbol = $ConfigContent.symbol
        }
    } catch {
        Write-Warning "Não foi possível analisar $ResolvedConfigPath. Usando padrões."
    }
}

# Montar argumentos do health checker
$ArgsList = @("-m", "src.health_checker")
if ($Json) {
    $ArgsList += "--json"
}
if ($TargetSymbol) {
    $ArgsList += "--symbol"
    $ArgsList += $TargetSymbol
}

try {
    & $PythonCmd $ArgsList
    $ExitCode = $LASTEXITCODE
    exit $ExitCode
} catch {
    Write-Error "Falha ao executar src.health_checker com Python: $_"
    exit 2
}
