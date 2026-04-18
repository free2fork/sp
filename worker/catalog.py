import os
import duckdb

def open_db():
    con = duckdb.connect()
    # Extensions are pre-installed in Docker image — just LOAD (skip INSTALL check)
    con.execute("LOAD httpfs;")
    con.execute("LOAD iceberg;")
    
    # Inject S3 credentials from Fly.io / Environment
    endpoint = os.environ.get('AWS_ENDPOINT_URL_S3', 'fly.storage.tigris.dev').replace('https://', '').replace('http://', '')
    con.execute(f"SET s3_endpoint='{endpoint}'")
    con.execute(f"SET s3_access_key_id='{os.environ.get('AWS_ACCESS_KEY_ID', '')}'")
    con.execute(f"SET s3_secret_access_key='{os.environ.get('AWS_SECRET_ACCESS_KEY', '')}'")
    
    if "minio" in os.environ.get('AWS_ENDPOINT_URL_S3', ''):
        con.execute("SET s3_use_ssl=false;")
        con.execute("SET s3_url_style='path';")
    else:
        con.execute("SET s3_use_ssl=true;")
        con.execute("SET s3_url_style='vhost';")
        
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET enable_http_metadata_cache=true;")
    con.execute("SET enable_object_cache=true;")
    # Allow iceberg_scan() to find metadata without version-hint.text file
    # Safe in our case since the coordinator manages all Iceberg commits
    con.execute("SET unsafe_enable_version_guessing=true;")

    # ── S3/HTTP performance tuning ──
    # Apply optional performance settings (may not exist in all DuckDB versions)
    for setting in [
        "SET http_keep_alive=true;",
    ]:
        try:
            con.execute(setting)
        except Exception:
            pass

    # Use ~80% of system RAM so DuckDB avoids spilling to the tiny Fly disk
    try:
        total_mem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
        limit_gb = max(int(total_mem * 0.8) // (1024**3), 1)
        con.execute(f"SET memory_limit='{limit_gb}GB';")
    except Exception:
        pass  # Fall back to DuckDB default if sysconf unavailable
    
    # Parquet Catalog setup: Instead of a massive frozen.ddb SQLite snapshot,
    # we point to the parquet files directly in Tigris, allowing DuckDB
    # to perform HTTP byte-range scans for minimal metadata overhead at boot.
    return con
