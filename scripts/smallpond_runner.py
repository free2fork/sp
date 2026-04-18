#!/usr/bin/env python3
"""
smallpond_runner.py
Standalone CLI for running batch SQL jobs via smallpond against Tigris Parquet files.
Use this for heavy batch processing outside the HTTP API (e.g. cron jobs, CI pipelines).

Usage:
    python smallpond_runner.py --sql "SELECT col, COUNT(*) FROM {0} GROUP BY col" \\
                               --output s3://duckpond-data/results/out.parquet \\
                               --workers 8 \\
                               --partition-by hash

smallpond dispatches each partition to a separate DuckDB Ray task.
Results are written back to Tigris as Parquet via write_parquet().
"""
import argparse
import logging
import os
import pathlib
import sys
import time

import smallpond
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger("smallpond_runner")

# Tigris credentials (read from env / .env)
TIGRIS_ENDPOINT = os.environ["AWS_ENDPOINT_URL_S3"]
TIGRIS_KEY      = os.environ["AWS_ACCESS_KEY_ID"]
TIGRIS_SECRET   = os.environ["AWS_SECRET_ACCESS_KEY"]
BUCKET_NAME     = os.environ["BUCKET_NAME"]
DATA_PREFIX     = os.environ.get("DATA_PREFIX", "parquet/")


def list_parquet_files(prefix: str) -> list[str]:
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=TIGRIS_ENDPOINT,
        aws_access_key_id=TIGRIS_KEY,
        aws_secret_access_key=TIGRIS_SECRET,
        region_name="auto",
    )
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(f"s3://{BUCKET_NAME}/{obj['Key']}")
    return files


def run_job(
    sql_template: str,
    output_uri: str,
    workers: int,
    partition_by: str,
    partition_col: str | None,
    dry_run: bool,
):
    files = list_parquet_files(DATA_PREFIX)
    if not files:
        log.error("No Parquet files found under s3://%s/%s", BUCKET_NAME, DATA_PREFIX)
        sys.exit(1)

    log.info("Found %d Parquet files", len(files))
    log.info("Initialising smallpond session…")

    # Configure S3 credentials for smallpond/DuckDB via env so Ray workers inherit them
    os.environ.setdefault("AWS_ENDPOINT_URL_S3",  TIGRIS_ENDPOINT)
    os.environ.setdefault("AWS_ACCESS_KEY_ID",    TIGRIS_KEY)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY",TIGRIS_SECRET)
    os.environ.setdefault("AWS_REGION",           "auto")

    sp = smallpond.init()
    df = sp.read_parquet(files)

    # ── partition strategy ────────────────────────────────────────────────────
    if partition_by == "hash":
        df = df.repartition(workers, hash_by=partition_col)
    elif partition_by == "column" and partition_col:
        df = df.repartition(workers, partition_by=partition_col)
    else:
        df = df.repartition(workers)

    log.info("Partition strategy: %s (col=%s) → %d workers", partition_by, partition_col, workers)

    # ── apply SQL (partial_sql injects partition files as {0}) ───────────────
    result = sp.partial_sql(sql_template, df)

    if dry_run:
        log.info("[dry-run] Would write to %s — skipping materialisation", output_uri)
        return

    # ── write output ──────────────────────────────────────────────────────────
    t0 = time.time()
    log.info("Materialising results → %s", output_uri)
    result.write_parquet(output_uri)
    elapsed = round(time.time() - t0, 1)
    log.info("Done in %.1fs", elapsed)


def main():
    parser = argparse.ArgumentParser(description="Run a smallpond batch job over Tigris Parquet files")
    parser.add_argument("--sql",           required=True,
                        help="SQL template. Use {0} for the partition file reference, e.g. "
                             "'SELECT * FROM {0} WHERE region = \\'eu\\''")
    parser.add_argument("--output",        required=True,
                        help="S3 URI for output Parquet, e.g. s3://duckpond-data/results/")
    parser.add_argument("--workers",       type=int, default=4,
                        help="Number of parallel DuckDB workers (default: 4)")
    parser.add_argument("--partition-by",  choices=["hash", "column", "row"], default="hash")
    parser.add_argument("--partition-col", default=None,
                        help="Column to partition by (required for --partition-by column)")
    parser.add_argument("--data-prefix",   default=DATA_PREFIX,
                        help=f"S3 prefix to scan for Parquet files (default: {DATA_PREFIX})")
    parser.add_argument("--dry-run",       action="store_true",
                        help="Plan the job but skip writing output")
    args = parser.parse_args()

    global DATA_PREFIX
    DATA_PREFIX = args.data_prefix

    run_job(
        sql_template  = args.sql,
        output_uri    = args.output,
        workers       = args.workers,
        partition_by  = args.partition_by,
        partition_col = args.partition_col,
        dry_run       = args.dry_run,
    )


if __name__ == "__main__":
    main()
