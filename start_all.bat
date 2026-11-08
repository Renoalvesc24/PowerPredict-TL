@echo off
REM ============================================================
REM  PowerPredict TL - hahu sistema real-time hotu
REM  (XAMPP MySQL tenke ativu: XAMPP Control Panel -> MySQL Start)
REM  Fluxu: Bot -> FastAPI -> Kafka -> Spark Streaming -> MySQL -> Streamlit
REM  Seguru atu hala'o dala rua: komponente ne'ebe ativu ona la hahu fali.
REM ============================================================
cd /d "%~dp0"
title PowerPredict TL launcher
set "CHECK=powershell -NoProfile -ExecutionPolicy Bypass -File setup\check_running.ps1"

echo [1/6] Apache Kafka (KRaft) ...
%CHECK% -Pattern "\skafka\.Kafka\s" -Port 9092
if errorlevel 1 goto kafka_start
echo      ativu ona - la hahu dala rua.
goto api
:kafka_start
start "PowerPredict - Kafka" cmd /k call setup\start_kafka.bat
echo      hein 20 s ba Kafka ...
timeout /t 20 /nobreak >nul

:api
echo [2/6] FastAPI gateway  http://127.0.0.1:8000/docs ...
%CHECK% -Pattern "uvicorn api\.main" -Port 8000
if errorlevel 1 goto api_start
echo      ativu ona - la hahu dala rua.
goto stream
:api_start
start "PowerPredict - API" cmd /k python -W ignore -m uvicorn api.main:app --host 127.0.0.1 --port 8000
timeout /t 5 /nobreak >nul

:stream
echo [3/6] Spark Streaming (Kafka -^> Spark -^> Random Forest -^> MySQL) ...
%CHECK% -Pattern "pipeline\.spark_streaming"
if errorlevel 1 goto stream_start
echo      ativu ona - la hahu dala rua.
goto bot
:stream_start
start "PowerPredict - Spark Streaming" cmd /k python -W ignore -m pipeline.spark_streaming --engine spark --from-latest

:bot
echo [4/6] Meter bot (281 meters, reading kada 2 s) ...
%CHECK% -Pattern "bot\.bot_live"
if errorlevel 1 goto bot_start
echo      ativu ona - la hahu dala rua.
goto dashboard
:bot_start
start "PowerPredict - Bot" cmd /k python -W ignore -m bot.bot_live

:dashboard
echo [5/6] Dashboard Streamlit  http://localhost:8505 ...
%CHECK% -Pattern "dashboard[\\/]app\.py" -Port 8505
if errorlevel 1 goto dashboard_start
echo      ativu ona - la hahu dala rua.
goto browser
:dashboard_start
start "PowerPredict - Dashboard" cmd /k python -W ignore -m streamlit run dashboard\app.py --server.port 8505
timeout /t 8 /nobreak >nul

:browser
start "" http://localhost:8505

:public
echo [6/6] Website publiku (internet, konvidadu de'it) + link permanente ...
echo      https://renoalvesc24.github.io/PowerPredict-TL/
powershell -NoProfile -ExecutionPolicy Bypass -File setup\public_site.ps1

echo.
echo Komponente hotu hahu ona iha janela rasik. Taka janela (ka Ctrl+C) atu para.
