FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    duckdb==1.5.1 \
    pyarrow \
    fastapi \
    uvicorn \
    boto3

COPY . /app

# Completely bake required external connectors and httpfs binaries statically into the container image
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL postgres; INSTALL mysql; INSTALL iceberg; INSTALL delta; INSTALL httpfs;')"

# Pre-compile all Python bytecode to skip .pyc generation at runtime
RUN python -m compileall -q /app /usr/local/lib/python3.11

EXPOSE 8080

CMD ["python", "worker.py"]
