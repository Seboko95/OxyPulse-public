@echo off
title OxyPulse – Live Trading System
cd /d "%~dp0"

echo.
echo  ==============================================
echo    OxyPulse v2 – Automated Crypto Trading
echo  ==============================================
echo.

:: Pre-flight checks
if not exist ".env" (
    echo  [ERROR] .env not found.
    echo  Run:  copy .env.example .env
    echo  Then fill in your API keys and Telegram token.
    echo.
    pause & exit /b 1
)

if not exist "logs"  mkdir logs
if not exist "data"  mkdir data

:: Dependencies
echo  [1/2] Checking dependencies...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo  [ERROR] pip install failed. Check your Python environment.
    pause & exit /b 1
)
echo        OK.

:: Main bot (foreground – Ctrl+C to stop)
echo  [2/2] Starting OxyPulse bot...
echo.
echo        Press Ctrl+C to stop.
echo.

python bot.py

echo.
echo  Bot stopped.
pause
