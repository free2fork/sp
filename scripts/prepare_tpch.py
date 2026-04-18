"""
Register TPC-H staging data as Iceberg tables in ShikiPond's catalog.
Run generate_tpch.py first to upload parquet files to Tigris.

Usage:
    export SHIKIPOND_TOKEN=...
    python3 prepare_tpch.py          # SF-1 (default)
    python3 prepare_tpch.py 10       # SF-10
"""
import os
import httpx
import sys
import json

SF = int(sys.argv[1]) if len(sys.argv) > 1 else 1
COORDINATOR_URL = os.environ.get("SHIKIPOND_URL", "https://duckpond-coordinator.fly.dev")
TOKEN = os.environ.get("SHIKIPOND_TOKEN")
BUCKET = os.environ.get("BUCKET_NAME", "duckpond-data")

if not TOKEN:
    print("Error: SHIKIPOND_TOKEN environment variable not set.")
    sys.exit(1)

client = httpx.Client(base_url=COORDINATOR_URL, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=None)

# TPC-H table schemas (DuckDB types)
TPCH_SCHEMAS = {
    "customer": """
        CREATE TABLE customer (
            c_custkey INTEGER, c_name VARCHAR, c_address VARCHAR,
            c_nationkey INTEGER, c_phone VARCHAR, c_acctbal DOUBLE,
            c_mktsegment VARCHAR, c_comment VARCHAR
        )""",
    "lineitem": """
        CREATE TABLE lineitem (
            l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER,
            l_linenumber INTEGER, l_quantity DOUBLE, l_extendedprice DOUBLE,
            l_discount DOUBLE, l_tax DOUBLE, l_returnflag VARCHAR,
            l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE,
            l_receiptdate DATE, l_shipinstruct VARCHAR, l_shipmode VARCHAR,
            l_comment VARCHAR
        )""",
    "nation": """
        CREATE TABLE nation (
            n_nationkey INTEGER, n_name VARCHAR, n_regionkey INTEGER, n_comment VARCHAR
        )""",
    "orders": """
        CREATE TABLE orders (
            o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus VARCHAR,
            o_totalprice DOUBLE, o_orderdate DATE, o_orderpriority VARCHAR,
            o_clerk VARCHAR, o_shippriority INTEGER, o_comment VARCHAR
        )""",
    "part": """
        CREATE TABLE part (
            p_partkey INTEGER, p_name VARCHAR, p_mfgr VARCHAR, p_brand VARCHAR,
            p_type VARCHAR, p_size INTEGER, p_container VARCHAR,
            p_retailprice DOUBLE, p_comment VARCHAR
        )""",
    "partsupp": """
        CREATE TABLE partsupp (
            ps_partkey INTEGER, ps_suppkey INTEGER, ps_availqty INTEGER,
            ps_supplycost DOUBLE, ps_comment VARCHAR
        )""",
    "region": """
        CREATE TABLE region (
            r_regionkey INTEGER, r_name VARCHAR, r_comment VARCHAR
        )""",
    "supplier": """
        CREATE TABLE supplier (
            s_suppkey INTEGER, s_name VARCHAR, s_address VARCHAR,
            s_nationkey INTEGER, s_phone VARCHAR, s_acctbal DOUBLE,
            s_comment VARCHAR
        )""",
}

def run_query(sql, label=None):
    display = label or sql[:100]
    print(f"  Executing: {display}...")
    try:
        resp = client.post("/query", json={
            "sql": sql,
            "compute_tier": "micro",
            "output_mode": "flight_stream"
        })
        if resp.status_code != 200:
            print(f"  Error: {resp.status_code} - {resp.text}")
            return False

        for line in resp.iter_lines():
            if not line: continue
            msg = json.loads(line)
            if msg.get("type") == "error":
                print(f"  x Failed: {msg.get('msg')}")
                return False
            if msg.get("type") == "done":
                break
        print(f"  Done.")
        return True
    except Exception as e:
        print(f"  Failed: {e}")
        return False

def prepare():
    tables = list(TPCH_SCHEMAS.keys())

    print(f"=== ShikiPond TPC-H SF-{SF} Catalog Setup ===\n")

    # Step 1: Drop existing catalog entries
    print("Step 1: Cleaning catalog...")
    for table in tables:
        run_query(f"DROP TABLE IF EXISTS {table}", label=f"DROP {table}")

    # Step 2: Create Iceberg tables
    print(f"\nStep 2: Creating Iceberg tables...")
    for table in tables:
        run_query(TPCH_SCHEMAS[table], label=f"CREATE TABLE {table}")

    # Step 3: Load staged parquet into Iceberg tables
    print(f"\nStep 3: Loading staged data into Iceberg tables...")
    for table in tables:
        s3_path = f"s3://{BUCKET}/staging/tpch_sf{SF}/{table}/part-0.parquet"
        sql = f"INSERT INTO {table} SELECT * FROM read_parquet('{s3_path}')"
        ok = run_query(sql, label=f"INSERT INTO {table}")
        if not ok:
            print(f"  x Failed to load {table}")

    print(f"\nDone! Run: python3 run_tpch_benchmark.py")

if __name__ == "__main__":
    prepare()
