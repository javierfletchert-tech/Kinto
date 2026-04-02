@echo off
chcp 65001 > nul
powershell -ExecutionPolicy Bypass -File "%~dp0publish-data.ps1" %*
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: el script termino con codigo %ERRORLEVEL%
    pause
) else (
    echo.
    echo Listo. Puedes cerrar esta ventana.
    pause
)
