import os
import httpx
import json

COORDINATOR_URL = os.environ.get("SHIKIPOND_URL", "https://duckpond-coordinator.fly.dev")
TOKEN = os.environ.get("SHIKIPOND_TOKEN")

client = httpx.Client(base_url=COORDINATOR_URL, headers={"Authorization": f"Bearer {TOKEN}"}, timeout=None)

def test_sql(sql):
    print(f"\nTesting SQL: {sql}")
    try:
        resp = client.post("/query", json={
            "sql": sql,
            "compute_tier": "standard",
            "output_mode": "json"
        })
        if resp.status_code != 200:
            print(f"Status Error: {resp.status_code} - {resp.text}")
            return
        
        for line in resp.iter_lines():
            if not line: continue
            msg = json.loads(line)
            if msg.get("type") == "error":
                print(f"Error: {msg.get('msg')}")
                return
            if msg.get("type") == "data":
                data = msg.get("data")
                if data:
                    print(f"Data Sample: {data[:1]}")
                else:
                    print("Data message received but 'data' field is empty.")
            if msg.get("type") == "done":
                break
        print("✓ Success.")
    except Exception as e:
        print(f"Failed: {e}")

print("--- Diagnostic Run (Detailed) ---")
test_sql("SELECT version()")
test_sql("INSTALL tpch; LOAD tpch; SELECT * FROM tpch_queries() LIMIT 5")
test_sql("INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.01)")
