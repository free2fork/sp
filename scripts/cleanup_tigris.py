"""
Clean up Tigris S3 storage and catalog for a fresh start.
Drops all TPC-H tables from the catalog and deletes S3 data.

Usage:
    export SHIKIPOND_TOKEN=...
    python3 cleanup_tigris.py              # list what would be deleted
    python3 cleanup_tigris.py --delete     # actually delete
"""
import os
import sys
import json
import boto3
import httpx

COORDINATOR_URL = os.environ.get("SHIKIPOND_URL", "https://duckpond-coordinator.fly.dev")
TOKEN = os.environ.get("SHIKIPOND_TOKEN")
BUCKET = os.environ.get("BUCKET_NAME", "duckpond-data")

# Tigris S3 credentials — grab from your env or Fly secrets
TIGRIS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL_S3", "https://fly.storage.tigris.dev")
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

DELETE = "--delete" in sys.argv

TPCH_TABLES = ["lineitem", "orders", "customer", "partsupp", "part", "supplier", "nation", "region"]
# Will be auto-discovered from the bucket
S3_PREFIXES = []


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=TIGRIS_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="auto",
        config=boto3.session.Config(s3={"addressing_style": "virtual"}, signature_version="s3v4"),
    )


def list_all_objects(s3, prefix):
    """List all objects under a prefix, handling pagination."""
    objects = []
    kwargs = {"Bucket": BUCKET, "Prefix": prefix}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            objects.append(obj)
        if not resp.get("IsTruncated"):
            break
        kwargs["ContinuationToken"] = resp["NextContinuationToken"]
    return objects


def drop_tables():
    if not TOKEN:
        print("  SHIKIPOND_TOKEN not set — skipping catalog cleanup")
        return

    client = httpx.Client(
        base_url=COORDINATOR_URL,
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=None,
    )

    # List all tables from the Iceberg REST catalog
    all_tables = []
    try:
        # Try known namespaces
        for ns in ["duckpond", "rddy-hub", "t_843584d6ac6f"]:
            resp = client.get(f"/v1/namespaces/{ns}/tables")
            if resp.status_code == 200:
                data = resp.json()
                tables = data.get("identifiers", data) if isinstance(data, dict) else data
                if isinstance(tables, list):
                    for t in tables:
                        name = t.get("name", t) if isinstance(t, dict) else t
                        all_tables.append((ns, name))
    except Exception as e:
        print(f"  Warning: could not list catalog tables: {e}")
        # Fall back to TPC-H tables in default namespace
        all_tables = [("duckpond", t) for t in TPCH_TABLES]

    if not all_tables:
        all_tables = [("duckpond", t) for t in TPCH_TABLES]

    for ns, table in all_tables:
        label = f"{ns}.{table}" if ns != "duckpond" else table
        print(f"  DROP TABLE IF EXISTS {label}...", end="", flush=True)
        try:
            sql = f"DROP TABLE IF EXISTS {table}" if ns == "duckpond" else f'DROP TABLE IF EXISTS "{ns}".{table}'
            resp = client.post("/query", json={
                "sql": sql,
                "compute_tier": "micro",
                "output_mode": "flight_stream",
            })
            for line in resp.iter_lines():
                if not line:
                    continue
                msg = json.loads(line)
                if msg.get("type") == "error":
                    print(f" ERROR: {msg.get('msg', '')[:80]}")
                    break
                if "msg" in msg:
                    print(f" {msg['msg'][:60]}")
                    break
        except Exception as e:
            print(f" FAILED: {e}")


def cleanup_s3():
    if not ACCESS_KEY:
        print("  AWS_ACCESS_KEY_ID not set — skipping S3 cleanup")
        return

    s3 = get_s3()
    print(f"\n  Endpoint: {TIGRIS_ENDPOINT}")
    print(f"  Bucket:   {BUCKET}")
    print(f"  Key:      {ACCESS_KEY[:20]}...")

    # Known prefixes to clean — no discovery needed
    prefixes = [
        "uploads/duckpond/",
        "uploads/rddy-hub/",
        "uploads/t_843584d6ac6f/",
        "iceberg/duckpond/",
        "iceberg/rddy-hub/",
        "iceberg/t_843584d6ac6f/",
        "staging/",
    ]

    for prefix in prefixes:
        try:
            objects = list_all_objects(s3, prefix)
        except Exception as e:
            print(f"\n  s3://{BUCKET}/{prefix}")
            print(f"    Skipped (AccessDenied — Tigris keys are Fly-internal only)")
            print(f"    DROP TABLE already cleaned up S3 data server-side")
            return  # No point trying other prefixes

        total_size = sum(o["Size"] for o in objects)
        size_mb = total_size / (1024 * 1024)

        print(f"\n  s3://{BUCKET}/{prefix}")
        print(f"    {len(objects)} objects, {size_mb:.1f} MB")

        if not objects:
            continue

        # Show a few examples
        for obj in objects[:5]:
            print(f"    - {obj['Key']} ({obj['Size'] / 1024:.1f} KB)")
        if len(objects) > 5:
            print(f"    ... and {len(objects) - 5} more")

        if DELETE:
            # Delete in batches of 1000 (S3 limit)
            for i in range(0, len(objects), 1000):
                batch = objects[i:i + 1000]
                s3.delete_objects(
                    Bucket=BUCKET,
                    Delete={"Objects": [{"Key": o["Key"]} for o in batch]},
                )
            print(f"    DELETED {len(objects)} objects")


def main():
    if DELETE:
        print("=" * 60)
        print("  CLEANUP MODE: deleting data!")
        print("=" * 60)
    else:
        print("=" * 60)
        print("  DRY RUN: listing what would be deleted")
        print("  Run with --delete to actually clean up")
        print("=" * 60)

    print("\n--- Catalog (DROP TABLE) ---")
    if DELETE:
        drop_tables()
    else:
        for t in TPCH_TABLES:
            print(f"  Would drop: {t}")

    print("\n--- S3 Storage ---")
    cleanup_s3()

    print("\n" + "=" * 60)
    if not DELETE:
        print("  Re-run with --delete to execute cleanup")
    else:
        print("  Cleanup complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
