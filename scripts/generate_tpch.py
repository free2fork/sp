"""
Generate TPC-H data locally as parquet files for manual upload to Tigris.

Usage:
    python3 generate_tpch.py          # SF-1 (default)
    python3 generate_tpch.py 10       # SF-10
    python3 generate_tpch.py 10 lineitem  # SF-10, only lineitem
"""
import os
import sys
import time
import duckdb
import math

SF = int(sys.argv[1]) if len(sys.argv) > 1 else 1
ONLY_TABLE = sys.argv[2] if len(sys.argv) > 2 else None
ALL_TABLES = ["region", "nation", "supplier", "part", "customer", "partsupp", "orders", "lineitem"]
TABLES = [ONLY_TABLE] if ONLY_TABLE else ALL_TABLES

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), f"tpch_sf{SF}")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Max chunk size (~500MB) — files larger than this get split
MAX_CHUNK_BYTES = 500 * 1024 * 1024

print(f"=== TPC-H SF-{SF} Local Generator ===")
print(f"Output directory: {OUTPUT_DIR}\n")

t0 = time.time()
con = duckdb.connect()
con.execute("INSTALL tpch; LOAD tpch;")
print(f"Generating TPC-H SF-{SF} data...")
con.execute(f"CALL dbgen(sf={SF});")
print(f"  dbgen complete in {time.time() - t0:.1f}s\n")

print("Exporting to parquet...")
for table in TABLES:
    t1 = time.time()
    row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    local_path = os.path.join(OUTPUT_DIR, f"{table}.parquet")
    con.execute(f"COPY {table} TO '{local_path}' (FORMAT 'parquet');")
    con.execute(f"DROP TABLE {table};")

    file_size = os.path.getsize(local_path)
    size_mb = file_size / (1024 * 1024)

    if file_size <= MAX_CHUNK_BYTES:
        elapsed = time.time() - t1
        print(f"  {table:>10} | {row_count:>12,} rows | {size_mb:>8.1f} MB | {elapsed:.1f}s")
    else:
        # Split into chunks
        num_chunks = math.ceil(file_size / MAX_CHUNK_BYTES)
        rows_per_chunk = math.ceil(row_count / num_chunks)
        print(f"  {table:>10} | {row_count:>12,} rows | {size_mb:>8.1f} MB | splitting into {num_chunks} chunks...")

        for i in range(num_chunks):
            chunk_offset = i * rows_per_chunk
            chunk_path = os.path.join(OUTPUT_DIR, f"{table}_part{i}.parquet")
            con.execute(f"COPY (SELECT * FROM read_parquet('{local_path}') LIMIT {rows_per_chunk} OFFSET {chunk_offset}) TO '{chunk_path}' (FORMAT 'parquet');")
            chunk_size = os.path.getsize(chunk_path)
            print(f"             part {i+1}/{num_chunks} | {chunk_size / (1024**2):>8.1f} MB")

        os.remove(local_path)
        elapsed = time.time() - t1
        print(f"  {table:>10} | total: {elapsed:.1f}s")

con.close()
total = time.time() - t0

print(f"\nDone in {total:.1f}s.")
print(f"\nFiles are in: {OUTPUT_DIR}/")
print(f"Upload them to Tigris under: uploads/<your-namespace>/lineitem/")
