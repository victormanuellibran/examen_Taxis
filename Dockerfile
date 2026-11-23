FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    NYC_TAXI_DATA_DIR=/app/data \
    NYC_TAXI_DBT_WAREHOUSE=/app/data/warehouse/nyc_taxi_dbt.duckdb \
    NYC_TAXI_DBT_TEMP=/app/data/tmp \
    DBT_PROJECT_DIR=/app/dbt_project \
    DBT_PROFILES_DIR=/app/dbt_project \
    DBT_TARGET_PATH=/tmp/dbt_target \
    DBT_LOG_PATH=/tmp/dbt_logs

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --upgrade pip && \
    python -m pip install -r requirements.txt

COPY contracts/ ./contracts/
COPY dbt_project/ ./dbt_project/
COPY sql/ ./sql/
COPY src/ ./src/
COPY tests/ ./tests/
COPY AGENTS.md Makefile README.md RESULTADOS.md ./

CMD ["python", "-m", "src.pipeline", "--mes", "2025-06"]
