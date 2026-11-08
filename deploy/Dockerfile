# PowerPredict TL application image (FastAPI, Spark Streaming, meter bot, Streamlit) - linux/arm64 or amd64
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 TZ=Asia/Dili \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
    PP_DATA_DIR=/data
# Java 17 for Spark; tzdata so timestamps are Timor-Leste time like on the development PC
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps tzdata curl \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17-openjdk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY deploy/requirements.server.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY . /app
RUN mkdir -p /data /app/logs
