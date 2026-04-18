# DuckPond — distributed lakehouse on Fly.io + Tigris

Fly Machines (Firecracker) · Tigris (S3) · DuckLake · smallpond

---

## Prerequisites

```bash
# Install flyctl
curl -L https://fly.io/install.sh | sh
flyctl auth login

# Python 3.11+
pip install duckdb smallpond pyarrow boto3 httpx fastapi uvicorn python-dotenv
```

---

## Step 1 — Bootstrap the Fly.io stack

```bash
# From the project root
bash scripts/setup.sh iad    # replace iad with your nearest region
                              # see: fly platform regions
```

This script:
1. Creates two Fly apps: `duckpond-worker` and `duckpond-coordinator`
2. Provisions a Tigris bucket (`duckpond-data`) and injects `AWS_*` secrets into both apps
3. Deploys the worker image (4 Machines, `performance-2x`)
4. Deploys the coordinator image (1 Machine, `shared-cpu-1x`)

---

## Step 2 — Get your Tigris credentials

```bash
fly secrets list -a duckpond-worker
# Copy AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET_NAME
```

Copy `.env.example` → `.env` and fill in the values.

---

## Step 3 — Open the UI

Open `frontend/index.html` directly in your browser (no build step needed).

In the **Setup** tab:
- Paste `https://duckpond-coordinator.fly.dev` as the Coordinator URL
- Paste the Tigris credentials from Step 2
- Click **Save & Test** — the status dot turns green

---

## Step 4 — Ingest your first dataset

In the **Ingest** tab:
- Source URI: `s3://your-existing-bucket/data/*.parquet`
- Table name: `events`
- Click **Start Ingest**

Then freeze the catalog so workers see it:

```bash
python scripts/freeze_catalog.py
```

---

## Step 5 — Run a query

In the **Query** tab, use `{files}` as the partition placeholder:

```sql
SELECT col, COUNT(*) AS n
FROM read_parquet([{files}])
GROUP BY col
ORDER BY n DESC
LIMIT 50
```

The coordinator fans this out to all workers, each reads its Parquet subset from
Tigris (which caches files regionally after first fetch), and results stream back
as Apache Arrow IPC.

---

## Project layout

```
duckpond/
├── worker/
│   ├── Dockerfile          # Firecracker VM image
│   ├── fly.toml            # Worker app config (auto-stop, scale to 0)
│   ├── worker.py           # FastAPI — receives partial_sql, returns Arrow IPC
│   └── catalog.py          # Fetches Frozen DuckLake .ddb from Tigris at boot
│
├── coordinator/
│   ├── Dockerfile
│   ├── fly.toml            # Coordinator app (always 1 machine alive)
│   ├── coordinator.py      # Scatter-gather, ingest API, schema proxy
│   └── tigris.py           # S3/Tigris helpers + DuckLake init
│
├── frontend/
│   └── index.html          # Single-file UI — no build step
│
├── scripts/
│   ├── setup.sh            # One-shot Fly.io + Tigris bootstrap
│   └── freeze_catalog.py   # Snapshot DuckLake catalog → Tigris
│
└── .env.example
```

---

## Scaling

```bash
# Add more workers
fly scale count 8 -a duckpond-worker

# Bigger workers for heavy scans
fly scale vm performance-4x -a duckpond-worker

# Update worker count in coordinator
fly secrets set WORKER_COUNT=8 -a duckpond-coordinator
```

---

## Cost estimate (4 workers, ~10GB data)

| Component         | Cost/month        |
|-------------------|-------------------|
| 4× `perf-2x` VMs | ~$0 when idle (auto-stop) |
| 1× coordinator    | ~$2 (shared-cpu-1x, always on) |
| Tigris 10GB       | ~$0.20 |
| Tigris GET reqs   | ~$0.05 |
| **Total**         | **~$2.25/month**  |

Workers scale to zero between jobs — you only pay for compute during active queries.
# sp-test
