"""
Upload locally-generated TPC-H parquet files to ShikiPond via the coordinator.
Two-phase approach: stage all files to Tigris first, then create Iceberg table in one go.

Usage:
    export SHIKIPOND_TOKEN=...
    python3 upload_tpch.py .                        # all files in current dir
    python3 upload_tpch.py . lineitem               # only lineitem
    python3 upload_tpch.py /path/to/tpch_sf10       # from any directory
"""
import os
import sys
import time
import glob
import httpx

if len(sys.argv) < 2:
    print("Usage: python3 upload_tpch.py <folder> [table_name]")
    sys.exit(1)

FOLDER = sys.argv[1]
ONLY_TABLE = sys.argv[2] if len(sys.argv) > 2 else None
COORDINATOR_URL = os.environ.get("SHIKIPOND_URL", "https://duckpond-coordinator.fly.dev")
TOKEN = os.environ.get("SHIKIPOND_TOKEN")

if not TOKEN:
    print("Error: SHIKIPOND_TOKEN environment variable not set.")
    sys.exit(1)

if not os.path.isdir(FOLDER):
    print(f"Error: {FOLDER} is not a directory.")
    sys.exit(1)

client = httpx.Client(
    base_url=COORDINATOR_URL,
    headers={"Authorization": f"Bearer {TOKEN}"},
    timeout=None,
)

# Discover parquet files and group by table name
files = sorted(glob.glob(os.path.join(FOLDER, "*.parquet")))
if not files:
    print(f"No .parquet files found in {FOLDER}")
    sys.exit(1)

# Group files by table name
tables = {}
for f in files:
    basename = os.path.basename(f).replace(".parquet", "")
    if "_part" in basename:
        table_name = basename.rsplit("_part", 1)[0]
    else:
        table_name = basename
    tables.setdefault(table_name, []).append(f)

if ONLY_TABLE:
    if ONLY_TABLE not in tables:
        print(f"Error: no files found for table '{ONLY_TABLE}'. Available: {', '.join(tables.keys())}")
        sys.exit(1)
    tables = {ONLY_TABLE: tables[ONLY_TABLE]}

print(f"=== ShikiPond Parquet Uploader ===")
print(f"Folder: {FOLDER}")
print(f"Tables: {', '.join(tables.keys())}\n")

for table_name, parts in tables.items():
    t0 = time.time()
    total_size = sum(os.path.getsize(f) for f in parts)
    print(f"  {table_name:>10} | {len(parts)} file(s) | {total_size / (1024**2):.1f} MB total")

    if len(parts) == 1:
        # Single file — use /upload directly (simpler, one operation)
        filepath = parts[0]
        file_size = os.path.getsize(filepath)
        print(f"             uploading {os.path.basename(filepath)} ({file_size / (1024**2):.1f} MB)...", end="", flush=True)
        with open(filepath, "rb") as f:
            resp = client.post(
                "/upload",
                files={"file": (f"{table_name}.parquet", f, "application/octet-stream")},
                data={"table_name": table_name},
            )
        elapsed = time.time() - t0
        if resp.status_code == 200:
            print(f" OK ({elapsed:.1f}s)")
        else:
            print(f" FAILED ({resp.status_code}): {resp.text[:200]}")
    else:
        # Multiple files — two-phase: stage all, then create table from all at once
        staged_uris = []

        # Phase 1: Stage all files to Tigris
        print(f"             Phase 1: Staging {len(parts)} files to Tigris...")
        for filepath in sorted(parts):
            file_size = os.path.getsize(filepath)
            print(f"               staging {os.path.basename(filepath)} ({file_size / (1024**2):.1f} MB)...", end="", flush=True)
            with open(filepath, "rb") as f:
                resp = client.post(
                    "/stage",
                    files={"file": (os.path.basename(filepath), f, "application/octet-stream")},
                    data={"table_name": table_name},
                )
            if resp.status_code == 200:
                data = resp.json()
                if "uri" in data:
                    staged_uris.append(data["uri"])
                    print(f" OK ({time.time() - t0:.1f}s)")
                else:
                    print(f" FAILED: {data}")
            else:
                print(f" FAILED ({resp.status_code}): {resp.text[:200]}")

        if len(staged_uris) != len(parts):
            print(f"             Only {len(staged_uris)}/{len(parts)} files staged. Aborting.")
            continue

        # Phase 2: Create Iceberg table from all staged files
        print(f"             Phase 2: Creating Iceberg table from {len(staged_uris)} staged files...", end="", flush=True)
        resp = client.post(
            "/upload-from-staged",
            json={"table_name": table_name, "uris": staged_uris},
        )
        elapsed = time.time() - t0
        if resp.status_code == 200:
            data = resp.json()
            if "error" in data:
                print(f" FAILED: {data['error']}")
            else:
                print(f" OK ({elapsed:.1f}s)")
        else:
            print(f" FAILED ({resp.status_code}): {resp.text[:200]}")

    elapsed = time.time() - t0
    print(f"  {table_name:>10} | done in {elapsed:.1f}s\n")

print("Done. Run: python3 run_tpch_benchmark.py")
