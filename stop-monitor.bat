@echo off
setlocal
cd /d "%~dp0"

echo =================================================
echo   WIN MARKET MONITOR - ENCERRAMENTO SEGURO
echo =================================================

powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\stop-monitor.ps1" %*
set EXIT_CODE=%ERRORLEVEL%

if %EXIT_CODE% neq 0 (
    echo.
    echo [ATENÇÃO] O encerramento encerrou com código de erro %EXIT_CODE%.
    echo Pressione qualquer tecla para fechar esta janela...
    pause >nul
)

exit /b %EXIT_CODE%
