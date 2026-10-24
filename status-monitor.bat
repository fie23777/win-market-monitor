@echo off
setlocal
cd /d "%~dp0"

echo =================================================
echo   WIN MARKET MONITOR - VERIFICAÇÃO DE SAÚDE
echo =================================================

powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\scripts\check-monitor-health.ps1" %*
set EXIT_CODE=%ERRORLEVEL%

echo.
echo Pressione qualquer tecla para fechar esta janela...
pause >nul

exit /b %EXIT_CODE%
