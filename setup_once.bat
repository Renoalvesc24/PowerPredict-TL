@echo off
REM ============================================================
REM  PowerPredict TL - setup dala ida (~45 minutu)
REM  Presiza: Python 3.11, Java 17, XAMPP MySQL ativu, Kafka iha C:\kafka
REM ============================================================
cd /d "%~dp0"
if not exist .env copy .env.example .env

echo [1/6] Pakote Python ...
python -m pip install -r requirements.txt || goto :error

echo [2/6] Dadus real seed: temperatura Open-Meteo 2025 ...
python -m data_generator.fetch_weather || goto :error

echo [3/6] Dataset Big Data 5GB, 10GB, 20GB (ho anomalia, ~55 GB disk - haree PP_DATA_DIR iha .env) ...
python -m data_generator.generate_dataset --size 5GB --workers 12 || goto :error
python -m data_generator.generate_dataset --size 10GB --workers 16 || goto :error
python -m data_generator.generate_dataset --size 20GB --workers 16 || goto :error

echo [4/6] Base de dadus MySQL, tabela no meters ...
python -W ignore -m setup.init_db --reset || goto :error

echo [5/6] Spark batch ETL (data lake + tabela servisu + sample treinu) ...
python -W ignore -m pipeline.spark_batch_etl --dataset 5GB || goto :error
python -W ignore -m pipeline.spark_batch_etl --dataset 10GB || goto :error
python -W ignore -m pipeline.spark_batch_etl --dataset 20GB || goto :error

echo [6/6] Modelu machine learning (Random Forest: scikit-learn + Spark MLlib) ...
python -W ignore -m ml.train --dataset 20GB || goto :error
set PP_SPARK_DRIVER_MEMORY=16g
python -W ignore -m pipeline.spark_ml_train --dataset 20GB --depth 16 --bins 64 --trees 60
python -W ignore dashboard\visualize_anomalies.py

echo.
echo Setup remata. Opsional: python -m pipeline.benchmark   (Pandas vs Spark, ~30 min)
echo Hahu sistema ho start_all.bat
goto :eof

:error
echo.
echo [ERROR] setup para - haree mensajen iha leten.
exit /b 1
