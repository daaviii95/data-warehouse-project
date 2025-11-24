# Stage 1: Ingestion container
FROM python:3.11-slim AS shopzada-ingest

# Install system deps for pandas/pyarrow/psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ingest.py /app/ingest.py
# Keep data folder mount-point; in docker compose we'll mount ./data -> /data
ENV DATA_DIR=/data
CMD ["python", "ingest.py"]

# Stage 2: Airflow container
FROM apache/airflow:2.10.2-python3.11 AS shopzada-airflow

USER root

# Copy requirements file as root (file operations allowed)
COPY requirements.txt /opt/airflow/requirements-shopzada.txt

# Switch to airflow user before installing packages (Airflow requirement)
USER airflow

# Reuse project dependencies inside Airflow environment so DAG tasks can call scripts/ingest.py
RUN pip install --no-cache-dir -r /opt/airflow/requirements-shopzada.txt
