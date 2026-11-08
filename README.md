# Sistema Deteksaun Anomalia Rede Eletrisidade Real Time uza metodu Random Forest (Capstone Project)
**PowerPredict TL - Big Data Capstone Project, Dili Institute of Technology (DIT) 2026**

## 👥 Membru Grupu 7
| Naran | NIM |
|---|---|
| Reinaldo N. M. Alves Costa | 2301020110084 |
| Alexandre E. Lisboa Ximenes | 2301020110003 |

Bacharelatu Siénsia Komputador, Dili Institute of Technology (DIT)

## 🛠️ Teknolojia ne'ebé Uza
Python 3.11, Apache Kafka 3.9 (KRaft), Apache Spark 3.5 (PySpark, Spark SQL, MLlib), FastAPI, scikit-learn (Random Forest),
MySQL / MariaDB (XAMPP), Streamlit + Altair, Parquet (Apache Arrow), Open-Meteo API

## 🏗️ Arkitetura Sistema
Fluxu real-time: **Bot → FastAPI → Kafka → Spark Streaming (feature + Random Forest) → MySQL → Streamlit**

Fluxu batch: **Jeradór dataset (5GB / 10GB / 20GB CSV) → Spark ETL → Parquet (Data Lake) → Random Forest (scikit-learn / Spark MLlib) → MySQL**

```
bot/bot_live.py ──POST──► api/main.py ──► Kafka (smart-meter-readings, partition 3)
 smart meter 281 + Open-Meteo   FastAPI         │
                                                ▼
                       pipeline/spark_streaming.py: validation · feature · window 1 min + watermark 2 min
                                                │   Random Forest (tipu anomalia + Anomaly Score)
                                                ▼
data_generator ──► pipeline/spark_batch_etl.py ──► MySQL powerpredict_db ──► dashboard/app.py (Streamlit, pájina 8)
 5GB/10GB/20GB       Parquet curated · training sample      ▲
                     ml/train.py · pipeline/spark_ml_train.py (Random Forest ativu)
```

## 🎯 Saida mak sistema deteta (Normal + tipu anomalia 6)
| Tipu anomalia | Saida mak akontese |
|---|---|
| Voltajen Tun | Voltage feeder tun 14-35 V (avaria, transformadór sobrekarga) |
| Voltajen Sa'e Liu | Voltage > +10 % iha kalan (karga ki'ik) |
| Frekuénsia La Estável | Generator trip - frequency sai husi 49.5-50.5 Hz |
| Karga Liu | Kliente uza liu kVA kontratu (load ratio > 1) |
| Na'ok Eletrisidade | Bypass meter - rejista de'it 5-55 % husi konsumu loos |
| Kontador Aat | Current sensor sala - I la tuir P/(V·PF) |

## 🧠 Métodu Machine Learning: Random Forest de'it
- **Tipu tarefa:** *supervised multi-class classification* (klase 7) + **Anomaly Score = 1 − P(Normal)** (binary).
- **Random Forest:** tree 150, max_depth 24, √feature kada split, Gini impurity, majority voting.
- **Tanba sa Random Forest de'it?** Iha test hanesan (dataset 20 GB, readings 1 millaun husi meters separadu),
  Random Forest mak accurate liu: F1 anomalia 0.954 kompara ho Decision Tree 0.925, Logistic Regression 0.896 no
  Isolation Forest 0.144. Tan ne'e sistema uza métodu ida de'it.
- **Class weight Normal = 4:** training set undersample Normal (56 % de'it), entaun weight 4 korrije prior ne'e
  (precision 0.850 → 0.942 iha test hanesan).
- **Feature 16:** voltage, current, power, power factor, frequency, temperature, kVA, hour, weekend, customer type,
  municipality + feature engineering: `load_ratio`, `voltage_dev_pct`, `freq_dev_hz`, **`baseline_ratio`**
  (power ÷ konsumu normal meter ne'e iha oras ne'e) no **`consistency_ratio`** (P ÷ V·I·PF).
- **Evaluation honestu:** training/test fahe **tuir meter** (1 husi 5 meters separadu), test set ho distribuisaun
  natural (~3.9 % anomalia), cross-validation 3-fold StratifiedGroupKFold.

### Rezultadu Random Forest (test iha meters separadu)
| Model | Training | Test | Precision | Recall | F1 anomalia | PR-AUC |
|---|---|---|---|---|---|---|
| **Random Forest #13 (scikit-learn, 20 GB, ativu)** | 1,079,003 | 999,536 | **0.942** | **0.968** | **0.954** | **0.990** |
| Random Forest #12 (Spark MLlib, 20 GB) | 2,298,650 | 36,958,516 | 0.785 | 0.968 | 0.867 | - |

Cross-validation: F1 anomalia 0.981 ± 0.001. Tipu susar liu: **Na'ok Eletrisidade** (F1 0.80) - uma-kain ne'ebé
uza eletrisidade uitoan parese ho na'ok, tan ne'e dashboard hatudu "suspeita" de'it bainhira meter ida repete.

## 📦 Dadus mai husi ne'ebé? (Data Source)
Dadus **sintétiku ho seed reál** (la'ós dadus kliente EDTL loos):
1. **UCI ElectricityLoadDiagrams20112014** - padraun konsumu fulan-fulan husi kliente reál 264.
2. **Open-Meteo** - temperature reál kada oras 2025 iha kapitál municipality 14 (batch) no temperature live (bot).
3. Meters aumenta to'o file CSV to'o **5 GB, 10 GB, 20 GB** (métodu aprezentasaun individuál / Exercicio 1).
4. Jeradór hatama **anomaly events** (ground truth) + problema Data Quality (0.3 % missing, 0.2 % duplicate, 0.1 % invalid).

| Dataset | Size | Records | Meters | Part file | % anomalia | Spark ETL |
|---|---|---|---|---|---|---|
| 5 GB | 5.09 GB | 45,590,438 | 5,194 | 52 | 3.80 % | 214 s |
| 10 GB | 10.20 GB | 91,180,877 | 10,388 | 104 | 3.79 % | 304 s |
| 20 GB | 20.49 GB | 182,361,755 | 20,776 | 208 | 3.80 % | 753 s |

### Pandas vs Spark (servisu hanesan, process foun kada run, média run 3)
| Volume | Pandas (s) | Spark (s) | Speedup Pandas/Spark | RAM Pandas | RAM Spark |
|---|---|---|---|---|---|
| 100k records | 0.87 | 11.39 | 0.08 | 0.1 GB | 1.7 GB |
| 1M records | 2.04 | 15.18 | 0.13 | 0.5 GB | 2.8 GB |
| 5 GB | ❌ out of memory | **50.46** | Spark de'it | > 20 GB | **12.8 GB** |
| 10 GB | ❌ out of memory | **81.16** | Spark de'it | > 20 GB | **12.9 GB** |
| 20 GB | ❌ out of memory | **189.38** | Spark de'it | > 20 GB | **13.0 GB** |

Pandas manán iha dadus ki'ik (Spark selu ~10 s hahú JVM). Maibé Pandas tenke karrega dataset tomak iha RAM:
iha komputador RAM 32 GB, Pandas la konsege prosesa **5 GB, 10 GB no 20 GB** (para iha ~20 GB RAM). Spark prosesa
partition-partition no spill ba disk, uza ~13 GB de'it ba dataset hotu → **dataset boot: Spark de'it**.

## 🚀 Oinsá Atu Hala'o Projetu
1. Instala Python 3.11, Java 17, XAMPP no Kafka 3.9.1 iha `C:\kafka\kafka_2.13-3.9.1`.
2. Kopia `.env.example` ba `.env` (konfigurasaun MySQL / Kafka - la iha password iha kódigu).
   Dataset 5GB + 10GB + 20GB presiza ~55 GB disk: se C: la to'o, hili `PP_DATA_DIR` (ex. `D:\powerpredict-data`)
   no `PP_SPARK_LOCAL_DIR` iha `.env`.
3. XAMPP Control Panel → **MySQL Start**.
4. Setup dala ida (pakote, dataset, database, Spark ETL, Random Forest): `setup_once.bat`
5. Hahú sistema tomak: `start_all.bat` - ka ida-idak:
   - Kafka: `setup\start_kafka.bat`
   - API: `python -m uvicorn api.main:app --port 8000` (dokumentasaun: http://127.0.0.1:8000/docs)
   - Spark Streaming: `python -m pipeline.spark_streaming --engine spark`
   - Bot: `python -m bot.bot_live` (ka `python -m bot.bot_replay --dataset 5GB` atu replay dataset)
   - Dashboard: `streamlit run dashboard/app.py --server.port 8505` → http://localhost:8505

   `start_all.bat` seguru atu hala'o dala rua: komponente ne'ebé ativu ona la hahú fali
   (Kafka broker rua iha log dir ida estraga dadus Kafka). Se janela Kafka hatudu `CorruptRecordException`:
   taka janela Kafka, hala'o `setup\start_kafka.bat reset` (hamoos dadus Kafka de'it; dadus iha MySQL la lakon),
   depois hala'o fali `start_all.bat`.
6. Demo: pájina **Simulasaun** → *Voltajen Tun* / *Na'ok Eletrisidade* → haree **Dashboard**, **Monitor Real-Time**, **Anomaly Events**.

Komandu batch:
```bat
python -m data_generator.generate_dataset --size 20GB --workers 16
python -m pipeline.spark_batch_etl --dataset 20GB
python -m ml.train --dataset 20GB
set PP_SPARK_DRIVER_MEMORY=16g
python -m pipeline.spark_ml_train --dataset 20GB --depth 16 --bins 64 --trees 60
python -m pipeline.benchmark --runs 3
python dashboard/visualize_anomalies.py
python -m pytest tests -q
```

## 🖥️ Website (Streamlit, pájina 9 + login)
**Login:** tab **User** no tab **Admin**. Konta dahuluk kria dala ida husi `.env`
(`PP_ADMIN_USERNAME` / `PP_ADMIN_PASSWORD`, `PP_USER_USERNAME` / `PP_USER_PASSWORD`, password karakter 8 ka liu);
depois Admin kria / jere konta iha pájina **Jestaun Uzuáriu**. Password rai hanesan hash PBKDF2-SHA256
(`tbl_users`), sesaun oras 8 ho cookie (`tbl_user_sessions`) - la iha password iha kódigu.
Kada pájina iha butaun **Refresh** (karrega dadus foun agora) no sidebar iha **Night mode / Day mode**.

| Seksaun | Pájina | Konteúdu | Role |
|---|---|---|---|
| Monitorizasaun | Dashboard | Alerta → Análitika Deskritiva → Tendénsia → Estatístika → Anomaly Score → Klasifikasaun tipu → Real-time (refresh 10 s) | User, Admin |
| | Monitor Real-Time | Mapa meters, estadu kada municipality, feed anomalia, voltage no frequency (refresh 5 s) | User, Admin |
| | Anomaly Events | Filter, download CSV, lista suspeita Na'ok Eletrisidade · **konfirma events: Admin** | User, Admin |
| Análize | Deteksaun Manual | Hatama reading → tipu anomalia, votu tree 150, evidénsia | User, Admin |
| | Model Random Forest | Rezultadu test, tanba sa Random Forest, Confusion Matrix, threshold · **retrain / ativa model: Admin** | User, Admin |
| | Big Data Pipeline | Dataset 5/10/20 GB, Spark ETL, Data Quality, anomalia batch, Pandas vs Spark, streaming | User, Admin |
| Admin | Simulasaun | Hamosu Voltajen Tun, Voltajen Sa'e Liu, Frekuénsia La Estável, Karga Liu, Na'ok, Kontador Aat | Admin |
| | Jestaun Uzuáriu | Kria konta, muda role, reset password, ativa / dezativa, hamoos | Admin |
| Informasaun | Metodolojia | Data source, arkitetura, Random Forest (Gini, voting), feature | User, Admin |

## 🌐 Website Publiku (Internet)
**Link:** https://renoalvesc24.github.io/PowerPredict-TL/ (sistema live bainhira komputadór grupu ativu)

- `start_public.bat` (depois `start_all.bat`) hahú kópia website **konvidadu de'it** iha porta 8510
  (`PP_PUBLIC_GUEST=1`, `PP_PUBLIC_NO_LOGIN=1`: butaun *Tama hanesan konvidadu*, la iha login), tunnel Cloudflare
  (HTTPS, la presiza konta), no atualiza link permanente iha GitHub Pages (branch `gh-pages`) ba tunnel foun.
- Konvidadu haree Dashboard, Monitor, Events, Deteksaun Manual, Model, Big Data no Metodolojia; la iha asesu ba
  Simulasaun / Jestaun Uzuáriu, no deteksaun manual konvidadu la rai iha database. Admin uza `http://localhost:8505`.
- **24/7 iha server (Oracle Cloud Always Free, ARM):** `deploy/` iha Docker Compose (Kafka KRaft, MariaDB, FastAPI,
  Spark Streaming, bot, Streamlit, Caddy HTTPS). Passu: `deploy/ORACLE_SETUP.md` → `bash deploy/make_bundle.sh` →
  `deploy/remote_setup.sh <domain>` iha server. Internet hetan website (port 80/443) de'it.

## 📁 Estrutura Folder
```
api/             FastAPI gateway (main.py, schemas.py)
bot/             data source real-time: bot_live.py (smart meter simuladu), bot_replay.py (replay dataset)
pipeline/        spark_streaming.py, spark_batch_etl.py, spark_ml_train.py, benchmark.py
ml/              train.py (Random Forest), predict.py (serving)
dashboard/       app.py + views/ (pájina 9 + login), auth.py (login), ui.py (header, Refresh, Night mode, ikon), charts.py, ml_anomaly.py
data_generator/  fetch_weather.py, generate_dataset.py (5GB / 10GB / 20GB)
common/          domain.py (tipu anomalia, feature), db.py (table 19), spark.py
setup/           init_db.py, start_kafka.bat, check_running.ps1
data/samples/    sample_readings.csv (100 records, tipu hotu), sample_meters.csv
data/seed/       uci_real/ - dadus reál UCI (seed ba jeradór dataset)
tests/           test_anomaly.py
docs/            relatóriu
```

## 📊 Dataset Sampel
`data/samples/sample_readings.csv` (100 records: 52 Normal + 8 kada tipu anomalia) no `data/samples/sample_meters.csv`
ba test lalais. Dataset kompletu (6.3 GB) la tama iha repositóriu - jera fali ho `data_generator`.

## 🎥 Vídeu Demostrasaun
[Link YouTube Unlisted]

## 📝 Nota
- Naran kliente, meter no koordenada mak sintétiku - la'ós kliente EDTL reál.
- `true_type` iha stream mak ground truth husi bot - uza de'it atu avalia model, nunka sai input model.
- Spark iha Windows la iha winutils: projetu uza lista file explísitu, Parquet via Arrow (`mapInArrow`) no
  micro-batch ho offset Kafka iha MySQL. Ho winutils (ka Linux) `--engine structured` uza Structured Streaming direta.
- Kódigu versaun anterior rai iha `_archive/`.
