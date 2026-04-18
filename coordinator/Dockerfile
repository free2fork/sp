FROM python:3.11-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    duckdb==1.5.1 \
    smallpond \
    pyarrow \
    fastapi \
    uvicorn \
    httpx \
    boto3 \
    sqlglot \
    python-multipart \
    dbt-core \
    dbt-duckdb \
    stripe \
    "pyiceberg[pyarrow,s3fs,sql-sqlite]>=0.7.0"

COPY . /app

EXPOSE 8081

CMD ["python", "coordinator.py"]
