"""
ShikiPond TPC-H Benchmark Suite
Tests queries across compute tiers: micro, standard, enterprise.

Usage:
    export SHIKIPOND_TOKEN=...
    python3 run_tpch_benchmark.py                  # all tiers
    python3 run_tpch_benchmark.py micro             # single tier
    python3 run_tpch_benchmark.py micro standard    # specific tiers
"""
import os
import httpx
import time
import sys
import json

COORDINATOR_URL = os.environ.get("SHIKIPOND_URL", "https://duckpond-coordinator.fly.dev")
TOKEN = os.environ.get("SHIKIPOND_TOKEN")

if not TOKEN:
    print("Error: SHIKIPOND_TOKEN environment variable not set.")
    sys.exit(1)

client = httpx.Client(base_url=COORDINATOR_URL, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=None)

TIERS = {
    "micro":       "shared-cpu-1x / 256MB (persistent)",
    "standard":    "shared-cpu-2x / 2GB (ephemeral)",
    "enterprise":  "shared-cpu-8x / 16GB (ephemeral)",
    "distributed": "4x shared-cpu-4x / 8GB (fan-out)",
}

TPC_H_QUERIES = {
    "Q2": "SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment FROM part, supplier, partsupp, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND p_size = 15 AND p_type LIKE '%BRASS' AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE' AND ps_supplycost = (SELECT min(ps_supplycost) FROM partsupp, supplier, nation, region WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'EUROPE') ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100",
    "Q11": "SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY' GROUP BY ps_partkey HAVING sum(ps_supplycost * ps_availqty) > (SELECT sum(ps_supplycost * ps_availqty) * 0.0001 FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY') ORDER BY value DESC",
    "Q16": "SELECT p_brand, p_type, p_size, count(DISTINCT ps_suppkey) AS supplier_cnt FROM partsupp, part WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%' AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) AND ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%') GROUP BY p_brand, p_type, p_size ORDER BY supplier_cnt DESC, p_brand, p_type, p_size",
    "Q1": "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST('1998-09-02' AS DATE) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
    "Q3": "SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority FROM customer, orders, lineitem WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS DATE) AND l_shipdate > CAST('1995-03-15' AS DATE) GROUP BY l_orderkey, o_orderdate, o_shippriority ORDER BY revenue DESC, o_orderdate LIMIT 10",
    "Q6": "SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS DATE) AND l_shipdate < CAST('1995-01-01' AS DATE) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24",
}

def run_benchmark_query(name, sql, tier):
    print(f"  {name}...", end="", flush=True)
    start_time = time.time()
    try:
        resp = client.post("/query", json={
            "sql": sql,
            "compute_tier": tier,
            "output_mode": "flight_stream"
        })
        if resp.status_code != 200:
            print(f" FAILED ({resp.status_code})")
            return None

        row_count = 0
        first_row_time = None
        error_msg = None

        for line in resp.iter_lines():
            if not line: continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue

            if msg.get("type") == "error":
                error_msg = msg.get("msg", "Unknown error")
                break
            if msg.get("type") == "data":
                if first_row_time is None:
                    first_row_time = time.time() - start_time
                row_count += msg.get("row_count", 0)
            if msg.get("type") == "done":
                break

        if error_msg:
            print(f" FAILED: {error_msg[:80]}")
            return None

        total_time = time.time() - start_time
        ttfr = first_row_time or total_time
        print(f" {total_time:.2f}s (TTFR: {ttfr:.2f}s, {row_count} rows)")
        return {"query": name, "tier": tier, "total_time": total_time, "ttfr": ttfr, "rows": row_count}
    except Exception as e:
        print(f" FAILED: {e}")
        return None

def run_suite(tiers):
    # Detect scale factor by counting lineitem rows
    sf_label = "?"
    try:
        resp = client.post("/query", json={"sql": "SELECT COUNT(*) as cnt FROM lineitem", "compute_tier": "micro"})
        for line in resp.iter_lines():
            if not line: continue
            msg = json.loads(line)
            if msg.get("type") == "data" and msg.get("rows"):
                cnt = msg["rows"][0]["cnt"]
                if cnt > 50_000_000: sf_label = "SF-10"
                elif cnt > 5_000_000: sf_label = "SF-1"
                else: sf_label = f"~{cnt:,} rows"
    except: pass

    print("=" * 70)
    print(f"  ShikiPond TPC-H Benchmark Suite ({sf_label})")
    print("=" * 70)

    all_results = []

    for tier in tiers:
        print(f"\n--- {tier.upper()} tier ({TIERS[tier]}) ---")
        for name, sql in TPC_H_QUERIES.items():
            res = run_benchmark_query(name, sql, tier)
            if res:
                all_results.append(res)

    # Summary table
    print(f"\n{'=' * 70}")
    print(f"  RESULTS SUMMARY")
    print(f"{'=' * 70}")

    # Header row with tier columns
    tier_labels = {t: t.upper() for t in tiers}
    header = f"{'Query':<8}"
    for t in tiers:
        header += f" | {tier_labels[t]:>12}"
    print(header)
    print("-" * len(header))

    for qname in TPC_H_QUERIES:
        row = f"{qname:<8}"
        for t in tiers:
            match = next((r for r in all_results if r["query"] == qname and r["tier"] == t), None)
            if match:
                row += f" | {match['total_time']:>10.2f}s"
            else:
                row += f" | {'FAIL':>11}"
        print(row)

if __name__ == "__main__":
    requested = sys.argv[1:] if len(sys.argv) > 1 else list(TIERS.keys())
    valid = [t for t in requested if t in TIERS]
    if not valid:
        print(f"Invalid tier(s). Choose from: {', '.join(TIERS.keys())}")
        sys.exit(1)
    run_suite(valid)
