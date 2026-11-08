@echo off
REM ============================================================
REM  PowerPredict TL - website publiku (internet) husi komputador ne'e
REM  Hala'o start_all.bat uluk (Kafka, API, Spark, bot, MySQL).
REM  Link ba ema hotu: https://renoalvesc24.github.io/PowerPredict-TL/
REM  Website publiku = konvidadu de'it (la iha login). Admin uza http://localhost:8505.
REM ============================================================
cd /d "%~dp0"
title PowerPredict TL - public website
powershell -NoProfile -ExecutionPolicy Bypass -File setup\public_site.ps1
pause
