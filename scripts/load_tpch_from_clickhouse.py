"""
Load TPC-H data from ClickHouse's public S3 bucket into ShikiPond.
No local data needed — workers read directly from ClickHouse S3 → write to Tigris.

Usage:
    export SHIKIPOND_TOKEN=...
    python3 load_tpch_from_clickhouse.py 1        # SF-1
    python3 load_tpch_from_clickhouse.py 100      # SF-100
    python3 load_tpch_from_clickhouse.py 100 lineitem  # single table
"""
import os
import sys
import json
import time
import httpx

SF = int(sys.argv[1]) if len(sys.argv) > 1 else 1
ONLY_TABLE = sys.argv[2] if len(sys.argv) > 2 else None

COORDINATOR_URL = os.environ.get("SHIKIPOND_URL", "https://duckpond-coordinator.fly.dev")
TOKEN = os.environ.get("SHIKIPOND_TOKEN")

if not TOKEN:
    print("Error: SHIKIPOND_TOKEN environment variable not set.")
    sys.exit(1)

client = httpx.Client(
    base_url=COORDINATOR_URL,
    headers={"Authorization": f"Bearer {TOKEN}"},
    timeout=None,
)

# ClickHouse public S3 URLs
# SF-1: .tbl (uncompressed), SF >= 10: .tbl.gz (gzipped)
BASE_URL = "https://clickhouse-datasets.s3.amazonaws.com/h"

def s3_url(table):
    if SF == 1:
        return f"{BASE_URL}/1/{table}.tbl"
    else:
        return f"{BASE_URL}/{SF}/{table}.tbl.gz"

# TPC-H column definitions for read_csv (need explicit columns because .tbl has trailing |)
COLUMNS = {
    "nation": {
        "n_nationkey": "INTEGER", "n_name": "VARCHAR",
        "n_regionkey": "INTEGER", "n_comment": "VARCHAR",
    },
    "region": {
        "r_regionkey": "INTEGER", "r_name": "VARCHAR", "r_comment": "VARCHAR",
    },
    "supplier": {
        "s_suppkey": "INTEGER", "s_name": "VARCHAR", "s_address": "VARCHAR",
        "s_nationkey": "INTEGER", "s_phone": "VARCHAR", "s_acctbal": "DOUBLE",
        "s_comment": "VARCHAR",
    },
    "part": {
        "p_partkey": "INTEGER", "p_name": "VARCHAR", "p_mfgr": "VARCHAR",
        "p_brand": "VARCHAR", "p_type": "VARCHAR", "p_size": "INTEGER",
        "p_container": "VARCHAR", "p_retailprice": "DOUBLE", "p_comment": "VARCHAR",
    },
    "customer": {
        "c_custkey": "INTEGER", "c_name": "VARCHAR", "c_address": "VARCHAR",
        "c_nationkey": "INTEGER", "c_phone": "VARCHAR", "c_acctbal": "DOUBLE",
        "c_mktsegment": "VARCHAR", "c_comment": "VARCHAR",
    },
    "partsupp": {
        "ps_partkey": "INTEGER", "ps_suppkey": "INTEGER", "ps_availqty": "INTEGER",
        "ps_supplycost": "DOUBLE", "ps_comment": "VARCHAR",
    },
    "orders": {
        "o_orderkey": "INTEGER", "o_custkey": "INTEGER", "o_orderstatus": "VARCHAR",
        "o_totalprice": "DOUBLE", "o_orderdate": "DATE", "o_orderpriority": "VARCHAR",
        "o_clerk": "VARCHAR", "o_shippriority": "INTEGER", "o_comment": "VARCHAR",
    },
    "lineitem": {
        "l_orderkey": "INTEGER", "l_partkey": "INTEGER", "l_suppkey": "INTEGER",
        "l_linenumber": "INTEGER", "l_quantity": "DOUBLE", "l_extendedprice": "DOUBLE",
        "l_discount": "DOUBLE", "l_tax": "DOUBLE", "l_returnflag": "VARCHAR",
        "l_linestatus": "VARCHAR", "l_shipdate": "DATE", "l_commitdate": "DATE",
        "l_receiptdate": "DATE", "l_shipinstruct": "VARCHAR", "l_shipmode": "VARCHAR",
        "l_comment": "VARCHAR",
    },
}

# Load order: small tables first, big tables last
LOAD_ORDER = ["region", "nation", "supplier", "part", "customer", "partsupp", "orders", "lineitem"]

# Estimated row counts for progress display
ROW_ESTIMATES = {
    1:   {"lineitem": "6M",   "orders": "1.5M", "partsupp": "800K", "customer": "150K"},
    10:  {"lineitem": "60M",  "orders": "15M",  "partsupp": "8M",   "customer": "1.5M"},
    100: {"lineitem": "600M", "orders": "150M", "partsupp": "80M",  "customer": "15M"},
}

# Use enterprise tier for big tables, micro for small
BIG_TABLES = {"lineitem", "orders", "partsupp", "customer"}


def run_query(sql, tier="micro", label=None):
    display = label or sql[:120]
    print(f"  [{tier}] {display}")
    t0 = time.time()
    try:
        resp = client.post("/query", json={
            "sql": sql,
            "compute_tier": tier,
            "output_mode": "flight_stream",
        })
        if resp.status_code != 200:
            print(f"    ERROR {resp.status_code}: {resp.text[:200]}")
            return False

        row_count = 0
        for line in resp.iter_lines():
            if not line:
                continue
            msg = json.loads(line)
            if msg.get("type") == "error":
                print(f"    FAILED: {msg.get('msg', '')[:200]}")
                return False
            if msg.get("type") == "data":
                row_count += msg.get("row_count", 0)
            if msg.get("type") == "done":
                break

        elapsed = time.time() - t0
        print(f"    OK ({elapsed:.1f}s, {row_count} rows affected)")
        return True
    except Exception as e:
        print(f"    FAILED: {e}")
        return False


def build_read_csv(table):
    """Build a read_csv() expression for a ClickHouse TPC-H .tbl file."""
    url = s3_url(table)
    cols = COLUMNS[table]
    # Build columns dict: {'col_name': 'TYPE', ...}
    col_dict = ", ".join(f"'{k}': '{v}'" for k, v in cols.items())
    col_names = ", ".join(cols.keys())

    # read_csv with explicit columns, pipe delimiter, no header
    # Select only named columns to skip the trailing empty column from |
    return (
        f"SELECT {col_names} FROM read_csv('{url}', "
        f"delim='|', header=false, "
        f"columns={{{col_dict}}})"
    )


def load_table(table):
    tier = "enterprise" if (table in BIG_TABLES and SF >= 10) else "micro"
    est = ROW_ESTIMATES.get(SF, {}).get(table, "")
    est_str = f" (~{est} rows)" if est else ""

    print(f"\n--- {table}{est_str} ---")

    # Drop existing
    run_query(f"DROP TABLE IF EXISTS {table}", tier="micro", label=f"DROP TABLE IF EXISTS {table}")

    # Create table
    cols_sql = ", ".join(f"{k} {v}" for k, v in COLUMNS[table].items())
    create_sql = f"CREATE TABLE {table} ({cols_sql})"
    if not run_query(create_sql, tier="micro", label=f"CREATE TABLE {table}"):
        return False

    # Insert from ClickHouse S3
    read_expr = build_read_csv(table)
    insert_sql = f"INSERT INTO {table} {read_expr}"
    label = f"INSERT INTO {table} FROM clickhouse S3 (SF-{SF})"
    if not run_query(insert_sql, tier=tier, label=label):
        return False

    # Verify row count
    run_query(f"SELECT COUNT(*) as cnt FROM {table}", tier="micro", label=f"COUNT {table}")
    return True


def main():
    tables = [ONLY_TABLE] if ONLY_TABLE else LOAD_ORDER
    for t in tables:
        if t not in COLUMNS:
            print(f"Unknown table: {t}")
            sys.exit(1)

    print("=" * 70)
    print(f"  ShikiPond TPC-H SF-{SF} Loader (from ClickHouse public S3)")
    print(f"  Source: {BASE_URL}/{SF}/")
    print(f"  Target: {COORDINATOR_URL}")
    print("=" * 70)

    failed = []
    for table in tables:
        ok = load_table(table)
        if not ok:
            failed.append(table)

    print("\n" + "=" * 70)
    if failed:
        print(f"  FAILED tables: {', '.join(failed)}")
    else:
        print(f"  All {len(tables)} tables loaded successfully!")
        print(f"  Run: python3 run_tpch_benchmark.py")
    print("=" * 70)


if __name__ == "__main__":
    main()
