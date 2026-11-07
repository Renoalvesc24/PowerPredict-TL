@echo off
REM PowerPredict TL - start the Apache Kafka broker (KRaft mode, no ZooKeeper)
REM   setup\start_kafka.bat         start the broker
REM   setup\start_kafka.bat reset   wipe the Kafka data first (fix for CorruptRecordException)
setlocal
if "%PP_KAFKA_HOME%"=="" set "PP_KAFKA_HOME=C:\kafka\kafka_2.13-3.9.1"
set "CONFIG=%~dp0..\infra\kafka\server.properties"
set "LOGDIR=C:\kafka\data\powerpredict-kraft-logs"

if not exist "%PP_KAFKA_HOME%\bin\windows\kafka-server-start.bat" (
  echo [ERROR] Kafka not found in %PP_KAFKA_HOME%
  exit /b 1
)

REM Never start a second broker: on Windows two brokers on one log dir corrupt it.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0check_running.ps1" -Pattern "\skafka\.Kafka\s" -Port 9092
if errorlevel 1 goto not_running
echo Kafka is already running on localhost:9092 - not starting a second broker.
exit /b 0

:not_running
if /i not "%~1"=="reset" goto format
echo Wiping Kafka data in %LOGDIR% ...
if exist "%LOGDIR%" rmdir /s /q "%LOGDIR%"

:format
REM Format the KRaft storage once (first run, or after a reset)
if exist "%LOGDIR%\meta.properties" goto start
echo Formatting KRaft storage in %LOGDIR% ...
pushd "%PP_KAFKA_HOME%"
for /f "delims=" %%i in ('bin\windows\kafka-storage.bat random-uuid') do set "CLUSTER_ID=%%i"
echo Cluster id: %CLUSTER_ID%
call bin\windows\kafka-storage.bat format -t %CLUSTER_ID% -c "%CONFIG%"
popd

:start
REM Setting the heap avoids Kafka's "wmic" check (wmic is removed in new Windows 11 builds)
if "%KAFKA_HEAP_OPTS%"=="" set "KAFKA_HEAP_OPTS=-Xmx1G -Xms1G"
echo Starting Kafka broker on localhost:9092 ...
pushd "%PP_KAFKA_HOME%"
call bin\windows\kafka-server-start.bat "%CONFIG%"
popd
