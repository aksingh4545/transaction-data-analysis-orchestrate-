FROM apache/airflow:2.10.3-python3.11

USER root

RUN apt-get update && apt-get install -y \
    gcc \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
COPY --chown=airflow:root dbt /opt/airflow/dbt

RUN pip install --no-cache-dir -r requirements.txt