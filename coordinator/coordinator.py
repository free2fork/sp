import asyncio
import io
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Any

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
import smallpond
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form, Request, Depends
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import urllib.request
import stripe

import tigris
import auth as auth_module

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("coordinator")

app = FastAPI(title="duckpond-coordinator-v2")

ALLOWED_ORIGINS = os.environ.get("ALLOWED_ORIGINS", "https://duckpond-coordinator.fly.dev").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

WORKER_HOST    = os.environ.get("WORKER_FLY_HOST", "")
WORKER_PORT    = os.environ.get("WORKER_PORT", "8080")
WORKER_COUNT   = int(os.environ.get("WORKER_COUNT", "4"))
LOCAL_MODE     = not bool(WORKER_HOST)

if LOCAL_MODE:
    log.warning("WORKER_FLY_HOST not set — LOCAL mode (single-node smallpond)")

class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000
    table_name: str = "movies"
    output_mode: str = "flight_stream"
    compute_tier: str = "micro"
    secrets: Optional[list[dict]] = []

class IngestRequest(BaseModel):
    source_s3_uri: str
    table_name: str
    compute_tier: str = "distributed"

# -- Auth Dependency -----------------------------------------------------------
async def require_auth(request: Request) -> dict:
    """FastAPI dependency: verify JWT and return user dict with tenant namespace."""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    
    token = auth_header[7:]
    
    # Internal worker tokens bypass Supabase verification
    internal_token = os.environ.get("INTERNAL_AUTH_TOKEN", "")
    if internal_token and token == internal_token:
        return {"id": "system", "email": "system@internal", "namespace": "duckpond"}
    
    user = auth_module.verify_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
    # Check if user belongs to an org (SQLite-based)
    email = user.get("email", "")
    user_id = user.get("id", "")
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        row = conn.execute(
            "SELECT o.slug FROM org_members m JOIN organizations o ON m.org_id = o.id WHERE m.user_email = ? LIMIT 1",
            (email,)
        ).fetchone()
    if row:
        user["namespace"] = f"org_{row[0]}"
        user["org_slug"] = row[0]
    else:
        user["namespace"] = auth_module.get_tenant_namespace(user)
    
    return user

@app.get("/me")
async def me(user: dict = Depends(require_auth)):
    """Return current user info and tenant namespace."""
    info = auth_module.get_user_display(user)
    info["namespace"] = user.get("namespace")
    info["org_slug"] = user.get("org_slug")
    return info

_sp_session: Any = None

def get_sp() -> Any:
    global _sp_session
    if _sp_session is None:
        _sp_session = smallpond.init()
    return _sp_session

# Global call worker removed — injected directly into execution stream so hosts are dynamic.
def wake_workers():
    token = os.environ.get("FLY_API_TOKEN")
    if token: token = token.strip()
    if not token or LOCAL_MODE: return
    app_name = WORKER_HOST.split(".")[0]
    log.info(f"Checking cluster state for: {app_name}")
    try:
        req = urllib.request.Request(f"https://api.machines.dev/v1/apps/{app_name}/machines")
        req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req) as response:
            machines = json.loads(response.read().decode())
        
        woken = False
        for m in machines:
            if m.get("state") not in ["started", "starting"]:
                mid = m["id"]
                log.info(f"Waking machine {mid}...")
                start_req = urllib.request.Request(
                    f"https://api.machines.dev/v1/apps/{app_name}/machines/{mid}/start", 
                    method="POST"
                )
                start_req.add_header("Authorization", f"Bearer {token}")
                urllib.request.urlopen(start_req)
                woken = True
        
        if woken:
            log.info("Waiting 3s for Firecracker networking bounds...")
            time.sleep(3)
    except Exception as e:
        log.error(f"Failed to orchestrate machines: {e}")

# -- Warm Machine Pool ---------------------------------------------------------
import threading

class MachinePool:
    """
    Manages a pool of stopped Fly machines per compute tier.
    Starting a stopped machine is ~5-10x faster than creating a new one
    because it skips image pull and rootfs creation.
    """
    TIER_SPECS = {
        "standard":   {"cpus": 2, "memory_mb": 2048},
        "enterprise": {"cpus": 8, "memory_mb": 16384},
    }
    POOL_SIZE = 2  # machines per tier

    def __init__(self):
        self._lock = threading.Lock()
        self._pool: dict[str, list[str]] = {"standard": [], "enterprise": []}
        self._initialized = False

    def _fly_api(self, method, path, token, data=None):
        url = f"https://api.machines.dev{path}"
        req = urllib.request.Request(url, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        if data:
            req.add_header("Content-Type", "application/json")
            req.data = json.dumps(data).encode("utf-8")
        with urllib.request.urlopen(req, timeout=65) as resp:
            return json.loads(resp.read().decode())

    def ensure_pool(self, app_name, token):
        """Pre-provision stopped machines for each tier (idempotent)."""
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            try:
                machines = self._fly_api("GET", f"/v1/apps/{app_name}/machines", token)
                base_image = machines[0]["config"]["image"]
                base_env = machines[0]["config"].get("env", {})

                for tier, spec in self.TIER_SPECS.items():
                    # Find existing stopped pool machines for this tier
                    pool_machines = [
                        m["id"] for m in machines
                        if m.get("config", {}).get("metadata", {}).get("pool_tier") == tier
                        and m.get("state") in ("stopped", "suspended")
                    ]
                    self._pool[tier] = pool_machines
                    needed = self.POOL_SIZE - len(pool_machines)
                    if needed <= 0:
                        log.info(f"Pool[{tier}]: {len(pool_machines)} machines ready")
                        continue

                    log.info(f"Pool[{tier}]: creating {needed} standby machines...")
                    for _ in range(needed):
                        config = {
                            "region": "iad",
                            "config": {
                                "image": base_image,
                                "guest": {"cpu_kind": "shared", "cpus": spec["cpus"], "memory_mb": spec["memory_mb"]},
                                "auto_destroy": False,
                                "env": base_env,
                                "metadata": {"pool_tier": tier}
                            },
                            "skip_launch": True  # Create without starting
                        }
                        m_data = self._fly_api("POST", f"/v1/apps/{app_name}/machines", token, config)
                        self._pool[tier].append(m_data["id"])
                        log.info(f"Pool[{tier}]: created standby machine {m_data['id']}")

                self._initialized = True
            except Exception as e:
                log.error(f"Pool init failed: {e}")

    def acquire(self, tier, count, app_name, token):
        """
        Acquire `count` machines from the pool for the given tier.
        Starts them in parallel and returns list of (machine_id, host) tuples.
        Falls back to creating new machines if pool is empty.
        """
        with self._lock:
            available = self._pool.get(tier, [])
            acquired = available[:count]
            self._pool[tier] = available[count:]

        # Start acquired machines in parallel
        hosts = []
        started_ids = []

        def _start_machine(m_id):
            try:
                self._fly_api("POST", f"/v1/apps/{app_name}/machines/{m_id}/start", token)
                # Wait for started state
                url = f"/v1/apps/{app_name}/machines/{m_id}/wait?state=started&timeout=60"
                self._fly_api("GET", url, token)
                # Get machine info for IP
                m_info = self._fly_api("GET", f"/v1/apps/{app_name}/machines/{m_id}", token)
                private_ip = m_info.get("private_ip")
                if private_ip:
                    return (m_id, f"[{private_ip}]:8080")
                return (m_id, f"{m_id}.vm.{app_name}.internal:8080")
            except Exception as e:
                log.error(f"Failed to start pool machine {m_id}: {e}")
                return None

        # If not enough in pool, create extras on the fly
        extras_needed = count - len(acquired)
        if extras_needed > 0:
            log.info(f"Pool[{tier}]: need {extras_needed} extra machines (pool had {len(acquired)})")
            machines = self._fly_api("GET", f"/v1/apps/{app_name}/machines", token)
            base_image = machines[0]["config"]["image"]
            base_env = machines[0]["config"].get("env", {})
            spec = self.TIER_SPECS[tier]

            def _create_extra(_):
                config = {
                    "region": "iad",
                    "config": {
                        "image": base_image,
                        "guest": {"cpu_kind": "shared", "cpus": spec["cpus"], "memory_mb": spec["memory_mb"]},
                        "auto_destroy": True,
                        "env": base_env,
                        "metadata": {"pool_tier": tier, "ephemeral": "true"}
                    }
                }
                m_data = self._fly_api("POST", f"/v1/apps/{app_name}/machines", token, config)
                m_id = m_data["id"]
                url = f"/v1/apps/{app_name}/machines/{m_id}/wait?state=started&timeout=60"
                self._fly_api("GET", url, token)
                private_ip = m_data.get("private_ip")
                if private_ip:
                    return (m_id, f"[{private_ip}]:8080")
                return (m_id, f"{m_id}.vm.{app_name}.internal:8080")

            with ThreadPoolExecutor(max_workers=max(extras_needed, 1)) as pool:
                extra_results = list(pool.map(_create_extra, range(extras_needed)))
            for r in extra_results:
                if r:
                    started_ids.append(r[0])
                    hosts.append(r)

        # Start pool machines in parallel
        with ThreadPoolExecutor(max_workers=max(len(acquired), 1)) as pool:
            results = list(pool.map(_start_machine, acquired))

        for r in results:
            if r:
                started_ids.append(r[0])
                hosts.append(r)

        return hosts, started_ids

    def release(self, tier, machine_ids, app_name, token):
        """Stop machines and return them to the pool (instead of destroying)."""
        def _stop_machine(m_id):
            try:
                # Check if this was an ephemeral overflow machine
                m_info = self._fly_api("GET", f"/v1/apps/{app_name}/machines/{m_id}", token)
                if m_info.get("config", {}).get("metadata", {}).get("ephemeral") == "true":
                    # Destroy overflow machines
                    req = urllib.request.Request(
                        f"https://api.machines.dev/v1/apps/{app_name}/machines/{m_id}?force=true",
                        headers={"Authorization": f"Bearer {token}"},
                        method="DELETE"
                    )
                    urllib.request.urlopen(req)
                    log.info(f"Pool: destroyed overflow machine {m_id}")
                else:
                    # Stop pool machines for reuse
                    self._fly_api("POST", f"/v1/apps/{app_name}/machines/{m_id}/stop", token)
                    with self._lock:
                        if m_id not in self._pool.get(tier, []):
                            self._pool.setdefault(tier, []).append(m_id)
                    log.info(f"Pool[{tier}]: returned machine {m_id}")
            except Exception as e:
                log.error(f"Pool release failed for {m_id}: {e}")

        with ThreadPoolExecutor(max_workers=max(len(machine_ids), 1)) as pool:
            list(pool.map(_stop_machine, machine_ids))

_machine_pool = MachinePool()

# -- Warm Machine Cache --------------------------------------------------------
# Keeps enterprise machines running between queries so DuckDB's object_cache and
# http_metadata_cache survive across requests.  A background reaper destroys
# machines that have been idle longer than WARM_IDLE_SECONDS.

WARM_IDLE_SECONDS = int(os.environ.get("WARM_IDLE_SECONDS", "260"))  # 5 min default

class WarmMachineCache:
    def __init__(self):
        self._lock = threading.Lock()
        # tier -> list of {"id": str, "host": str, "last_used": float, "app": str, "token": str}
        self._warm: dict[str, list[dict]] = {}
        self._reaper_started = False

    def _start_reaper(self):
        if self._reaper_started:
            return
        self._reaper_started = True
        t = threading.Thread(target=self._reaper_loop, daemon=True)
        t.start()

    def _reaper_loop(self):
        """Background thread that destroys idle warm machines."""
        while True:
            time.sleep(30)  # check every 30s
            now = time.time()
            to_destroy = []
            with self._lock:
                for tier in list(self._warm.keys()):
                    still_warm = []
                    for entry in self._warm[tier]:
                        if now - entry["last_used"] > WARM_IDLE_SECONDS:
                            to_destroy.append(entry)
                        else:
                            still_warm.append(entry)
                    self._warm[tier] = still_warm

            for entry in to_destroy:
                try:
                    del_req = urllib.request.Request(
                        f"https://api.machines.dev/v1/apps/{entry['app']}/machines/{entry['id']}?force=true",
                        headers={"Authorization": f"Bearer {entry['token']}"},
                        method="DELETE"
                    )
                    urllib.request.urlopen(del_req)
                    log.info(f"WarmCache: reaped idle machine {entry['id']} (idle {now - entry['last_used']:.0f}s)")
                except Exception as e:
                    log.warning(f"WarmCache: failed to reap {entry['id']}: {e}")

    def acquire(self, tier: str) -> Optional[dict]:
        """Get a warm running machine for this tier, or None."""
        with self._lock:
            machines = self._warm.get(tier, [])
            if machines:
                entry = machines.pop(0)
                log.info(f"WarmCache: reusing warm machine {entry['id']} for {tier} "
                         f"(idle {time.time() - entry['last_used']:.1f}s)")
                return entry
        return None

    def release(self, tier: str, machine_id: str, host: str, app_name: str, token: str):
        """Return a machine to the warm cache instead of destroying it."""
        self._start_reaper()
        with self._lock:
            self._warm.setdefault(tier, []).append({
                "id": machine_id,
                "host": host,
                "last_used": time.time(),
                "app": app_name,
                "token": token,
            })
        log.info(f"WarmCache: kept machine {machine_id} warm for {tier} (TTL {WARM_IDLE_SECONDS}s)")

_warm_cache = WarmMachineCache()

# -- Distributed File Partitioning ---------------------------------------------

DISTRIBUTED_WORKER_COUNT = int(os.environ.get("DISTRIBUTED_WORKER_COUNT", "4"))
DISTRIBUTED_TIER_SPECS = {
    "cpus": 4, "memory_mb": 8192,  # 4 workers x 4 vCPU x 8GB = 16 vCPU + 32GB total
}

def _resolve_parquet_glob(glob_uri: str) -> list[str]:
    """Resolve an S3 glob like s3://bucket/path/*.parquet to individual file URIs."""
    import re
    match = re.match(r's3://([^/]+)/(.+?)(/\*\.parquet)$', glob_uri)
    if not match:
        return []
    bucket, prefix, _ = match.groups()
    prefix = prefix + "/"
    try:
        s3 = tigris.get_s3_client()
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = []
        for obj in resp.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(f"s3://{bucket}/{obj['Key']}")
        return sorted(files)
    except Exception as e:
        log.warning(f"Failed to resolve glob {glob_uri}: {e}")
        return []

def _partition_sql_for_fanout(sql: str, worker_count: int) -> list[str]:
    """
    Find the largest read_parquet glob in the SQL, resolve it to files,
    and produce N copies of the SQL each scanning a file subset.
    Small tables keep their full glob (broadcast to all workers).
    """
    import re
    # Find all read_parquet('s3://...*.parquet', ...) patterns
    pattern = r"read_parquet\('(s3://[^']*\*\.parquet)'(?:,\s*hive_partitioning\s*=\s*false)?\)"
    matches = list(re.finditer(pattern, sql))
    if not matches:
        return [sql]  # no globs found, single-worker fallback

    # Resolve each glob to find the largest table
    resolved = []
    for m in matches:
        glob_uri = m.group(1)
        files = _resolve_parquet_glob(glob_uri)
        resolved.append((m, glob_uri, files))

    # Pick the table with the most files to partition
    largest = max(resolved, key=lambda x: len(x[2]))
    _, largest_glob, largest_files = largest

    if len(largest_files) < worker_count:
        # Fewer files than workers — no point splitting
        return [sql]

    # Partition files round-robin across workers
    partitioned_sqls = []
    for w in range(worker_count):
        worker_files = largest_files[w::worker_count]
        if not worker_files:
            continue
        files_str = ", ".join(f"'{f}'" for f in worker_files)
        # Replace the glob with this worker's file list
        worker_sql = sql.replace(
            f"read_parquet('{largest_glob}', hive_partitioning = false)",
            f"read_parquet([{files_str}], hive_partitioning = false)"
        ).replace(
            f"read_parquet('{largest_glob}')",
            f"read_parquet([{files_str}])"
        )
        partitioned_sqls.append(worker_sql)

    log.info(f"Distributed fan-out: {len(largest_files)} files across {len(partitioned_sqls)} workers "
             f"(largest table: {largest_glob})")
    return partitioned_sqls

def _needs_ephemeral(sql: str, compute_tier: str, partition_count: int) -> bool:
    """
    Decide whether to spin up ephemeral VMs.
    - micro tier: never (always use persistent workers)
    - Data loads (INSERT, COPY, CALL dbgen, CREATE TABLE AS): yes, for standard/enterprise
    - Large partition sets: yes, when partition_count exceeds persistent fleet capacity
    - Small SELECTs: no, persistent workers handle it
    """
    if compute_tier == "micro":
        return False

    # Enterprise / Distributed tiers always get ephemeral VMs
    if compute_tier in ("enterprise", "distributed"):
        return True

    sql_upper = sql.strip().upper()

    # Data load operations need more RAM/CPU
    # Use specific patterns to avoid matching preamble (CREATE SECRET, ATTACH IF NOT EXISTS)
    if any(kw in sql_upper for kw in ["INSERT INTO", "\nCOPY ", "CALL DBGEN", "CREATE TABLE", "DROP TABLE"]):
        return True

    # Large datasets benefit from fan-out
    if partition_count > 4:
        return True

    return False

def stream_via_fly_workers(sql: str, partitions: list[str], limit: int, output_mode: str, compute_tier: str):
    def partition_list(lst, n):
        if not lst: return [[]]
        return [lst[i::n] for i in range(n)]

    # --- Distributed fan-out: partition SQL across multiple workers ---
    if compute_tier == "distributed":
        partitioned_sqls = _partition_sql_for_fanout(sql, DISTRIBUTED_WORKER_COUNT)
        active_worker_count = len(partitioned_sqls)
    else:
        partitioned_sqls = [sql]  # single-worker: one copy of the SQL
        active_worker_count = 1 if not partitions else len(partition_list(partitions, WORKER_COUNT))

    chunks = partition_list(partitions, WORKER_COUNT) if partitions else [[]] * active_worker_count
    job_id     = str(uuid.uuid4())
    app_name   = WORKER_HOST.split(".")[0]
    use_ephemeral = _needs_ephemeral(sql, compute_tier, len(partitions))

    def record_generator():
        raw_token = os.environ.get("FLY_API_TOKEN")
        token = raw_token.strip() if raw_token else ""
        ephemeral_machines = []  # newly created machines (destroy on failure)
        warm_reuse_ids = []      # machines borrowed from warm cache (return on success)
        target_hosts = []

        try:
            # Ephemeral Machine Provisioning for heavy workloads
            if use_ephemeral and token and not LOCAL_MODE:
                # --- Try warm cache first (grab as many as available) ---
                for _ in range(active_worker_count):
                    warm_entry = _warm_cache.acquire(compute_tier)
                    if warm_entry:
                        warm_reuse_ids.append(warm_entry["id"])
                        target_hosts.append(warm_entry["host"])
                    else:
                        break
                if warm_reuse_ids:
                    yield json.dumps({"type": "info", "msg": f"Reusing {len(warm_reuse_ids)} warm '{compute_tier}' instance(s) (cached)"}).encode('utf-8') + b"\n"

                # Create any additional machines needed beyond what warm cache provided
                needed = active_worker_count - len(target_hosts)
                if needed > 0:
                    yield json.dumps({"type": "info", "msg": f"Provisioning {needed} '{compute_tier}' instance(s)..."}).encode('utf-8') + b"\n"

                    req = urllib.request.Request(f"https://api.machines.dev/v1/apps/{app_name}/machines")
                    req.add_header("Authorization", f"Bearer {token}")
                    with urllib.request.urlopen(req) as resp:
                        machines = json.loads(resp.read().decode())

                    base_image = machines[0]["config"]["image"]
                    base_env = machines[0]["config"].get("env", {})

                    # Tier-specific VM sizing
                    if compute_tier == "distributed":
                        mem_mb = DISTRIBUTED_TIER_SPECS["memory_mb"]
                        cpus = DISTRIBUTED_TIER_SPECS["cpus"]
                    elif compute_tier == "standard":
                        mem_mb = 2048
                        cpus = 2
                    else:  # enterprise
                        mem_mb = 16384
                        cpus = 8
                    cpu_kind = os.environ.get("ENTERPRISE_CPU_KIND", "shared")

                    def _create_machine(_idx):
                        config = {
                            "region": "iad",
                            "config": {
                                "image": base_image,
                                "guest": {"cpu_kind": cpu_kind, "cpus": cpus, "memory_mb": mem_mb},
                                "auto_destroy": False,  # managed by warm cache reaper
                                "env": base_env
                            }
                        }
                        post_req = urllib.request.Request(
                            f"https://api.machines.dev/v1/apps/{app_name}/machines",
                            data=json.dumps(config).encode('utf-8'),
                            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                            method="POST"
                        )
                        with urllib.request.urlopen(post_req) as resp:
                            return json.loads(resp.read().decode())

                    with ThreadPoolExecutor(max_workers=max(needed, 1)) as create_pool:
                        futures = [create_pool.submit(_create_machine, i) for i in range(needed)]
                        for f in futures:
                            m_data = f.result()
                            m_id = m_data["id"]
                            ephemeral_machines.append(m_id)
                            private_ip = m_data.get("private_ip")
                            if private_ip:
                                target_hosts.append(f"[{private_ip}]:8080")
                            else:
                                target_hosts.append(f"{m_id}.vm.{app_name}.internal:8080")

                    yield json.dumps({"type": "info", "msg": f"Created {len(ephemeral_machines)} instance(s). Waiting for boot..."}).encode('utf-8') + b"\n"

                    # Wait for newly created machines (warm ones are already running)
                    def _wait_machine(m_id):
                        try:
                            wait_req = urllib.request.Request(
                                f"https://api.machines.dev/v1/apps/{app_name}/machines/{m_id}/wait?state=started&timeout=60",
                                headers={"Authorization": f"Bearer {token}"}
                            )
                            urllib.request.urlopen(wait_req, timeout=65)
                        except Exception as e:
                            log.warning(f"Wait for machine {m_id} failed: {e}")

                    with ThreadPoolExecutor(max_workers=max(len(ephemeral_machines), 1)) as wait_pool:
                        list(wait_pool.map(_wait_machine, ephemeral_machines))
            else:
                target_hosts = [f"{app_name}.internal:8080"] * active_worker_count
                # Trigger standard wakeup sequence for micro default
                if not LOCAL_MODE: wake_workers()

            if compute_tier == "distributed":
                yield json.dumps({"type": "info", "msg": f"cluster scattered ({len(target_hosts)} workers, fan-out)"}).encode('utf-8') + b"\n"
            else:
                yield json.dumps({"type": "info", "msg": "cluster scattered"}).encode('utf-8') + b"\n"

            # Isolated execution scope to handle independent host connection retries
            def _invoke_worker(idx, worker_sql, host):
                max_attempts = 20
                for attempt in range(max_attempts):
                    try:
                        client = flight.FlightClient(f"grpc://{host}")
                        ticket = flight.Ticket(json.dumps({
                            "sql": worker_sql,
                            "partition_id": str(idx),
                            "output_mode": output_mode,
                            "job_id": job_id
                        }).encode('utf-8'))
                        return client.do_get(ticket).read_all()
                    except Exception as e:
                        if attempt == max_attempts - 1:
                            raise Exception(f"Connection failed to {host} after {max_attempts} attempts: {e}")
                        backoff = min(0.5 * (2 ** attempt), 5.0)
                        time.sleep(backoff)

            total_rows = 0
            # Build per-worker SQL: distributed tier gets partitioned SQL, others use legacy {files} replacement
            worker_sqls = []
            for i in range(active_worker_count):
                if compute_tier == "distributed":
                    worker_sqls.append(partitioned_sqls[i])
                else:
                    chunk = chunks[i] if i < len(chunks) else []
                    worker_sqls.append(sql.replace("{files}", ", ".join(f"'{f}'" for f in chunk)))

            with ThreadPoolExecutor(max_workers=active_worker_count) as pool:
                future_to_idx = {
                    pool.submit(_invoke_worker, i, worker_sqls[i], target_hosts[i % len(target_hosts)]): i
                    for i in range(active_worker_count)
                }

                # Distributed tier: collect all Arrow tables for coordinator-side merge
                if compute_tier == "distributed" and active_worker_count > 1:
                    partial_tables = []
                    for future in as_completed(future_to_idx):
                        try:
                            table = future.result()
                            if table is not None and table.num_rows > 0:
                                partial_tables.append(table)
                        except Exception as exc:
                            log.error(f"Worker stream exception: {exc}")
                            yield json.dumps({"type": "error", "msg": str(exc)}).encode('utf-8') + b"\n"

                    if partial_tables:
                        # Merge partial results: re-run the original query over concatenated partials
                        combined = pa.concat_tables(partial_tables)
                        try:
                            merge_con = duckdb.connect()
                            merge_con.register("_partials", combined)
                            # Re-run the user's SELECT over the partial results to merge aggregates
                            # Extract only the core query (skip CREATE SECRET / ATTACH preamble)
                            import sqlglot
                            core_sql = None
                            for stmt in sqlglot.parse(sql, read="duckdb"):
                                if isinstance(stmt, sqlglot.exp.Select):
                                    core_sql = stmt
                                    break
                            if core_sql:
                                # Replace all table references with _partials
                                for tbl in core_sql.find_all(sqlglot.exp.Table):
                                    tbl.replace(sqlglot.parse_one("_partials", read="duckdb"))
                                merge_sql = core_sql.sql(dialect="duckdb")
                                merged = merge_con.execute(merge_sql).arrow()
                            else:
                                # Fallback: just use concatenated results
                                merged = combined
                            merge_con.close()
                        except Exception as merge_err:
                            log.warning(f"Distributed merge failed, using concatenated results: {merge_err}")
                            merged = combined

                        sliced = merged.slice(0, limit)
                        total_rows = sliced.num_rows
                        columns = sliced.schema.names
                        pydict = sliced.to_pydict()
                        rows = [{col: pydict[col][i] for col in columns} for i in range(sliced.num_rows)]

                        def _json_default(obj):
                            import datetime, decimal
                            if isinstance(obj, (datetime.date, datetime.datetime)):
                                return obj.isoformat()
                            if isinstance(obj, datetime.time):
                                return obj.isoformat()
                            if isinstance(obj, decimal.Decimal):
                                return float(obj)
                            if isinstance(obj, bytes):
                                return obj.hex()
                            return str(obj)

                        yield json.dumps({"type": "data", "columns": columns, "rows": rows, "row_count": total_rows}, default=_json_default).encode('utf-8') + b"\n"

                else:
                    # Single-worker path: stream results as they arrive
                    for future in as_completed(future_to_idx):
                        try:
                            table = future.result()
                            if table is None: continue
                            if total_rows >= limit: break

                            sliced = table.slice(0, limit - total_rows)
                            total_rows += sliced.num_rows

                            columns = sliced.schema.names
                            pydict = sliced.to_pydict()
                            rows = [{col: pydict[col][i] for col in columns} for i in range(sliced.num_rows)]

                            def _json_default(obj):
                                import datetime, decimal
                                if isinstance(obj, (datetime.date, datetime.datetime)):
                                    return obj.isoformat()
                                if isinstance(obj, datetime.time):
                                    return obj.isoformat()
                                if isinstance(obj, decimal.Decimal):
                                    return float(obj)
                                if isinstance(obj, bytes):
                                    return obj.hex()
                                return str(obj)

                            yield json.dumps({"type": "data", "columns": columns, "rows": rows, "row_count": sliced.num_rows}, default=_json_default).encode('utf-8') + b"\n"

                        except Exception as exc:
                            log.error(f"Worker stream exception: {exc}")
                            yield json.dumps({"type": "error", "msg": str(exc)}).encode('utf-8') + b"\n"

            yield json.dumps({"type": "done", "total_rows": total_rows}).encode('utf-8') + b"\n"

        except GeneratorExit:
            pass  # Client disconnected natively
        except urllib.error.HTTPError as http_exc:
            err_body = http_exc.read().decode()
            yield json.dumps({"type": "error", "msg": f"Fly.io REST API Error: {err_body}"}).encode('utf-8') + b"\n"
        except Exception as system_exc:
            yield json.dumps({"type": "error", "msg": str(system_exc)}).encode('utf-8') + b"\n"
        finally:
            # Return machines to warm cache (keeps them running for reuse)
            all_machine_ids = warm_reuse_ids + ephemeral_machines
            if all_machine_ids and token:
                for i, m_id in enumerate(all_machine_ids):
                    host = target_hosts[i] if i < len(target_hosts) else f"{m_id}.vm.{app_name}.internal:8080"
                    _warm_cache.release(compute_tier, m_id, host, app_name, token)
                log.info(f"Returned {len(all_machine_ids)} machine(s) to warm cache")

    return StreamingResponse(record_generator(), media_type="application/x-ndjson")

import httpx
import uuid
import time
import os
import sqlite3

# -- Dynamic Persistent Catalog Initialization ---------------------------------
if not os.path.exists("/data") and not os.environ.get("LOCAL_MODE"):
    log.warning("CRITICAL: /data mount not found! Catalog will use local ephemeral storage. Data may be lost on restart.")
CATALOG_DB_PATH = "/data/catalog.db" if os.path.exists("/data") else "catalog.db"
PYICEBERG_DB_PATH = "/data/pyiceberg_catalog.db" if os.path.exists("/data") else "pyiceberg_catalog.db"

def get_pyiceberg_catalog():
    try:
        from pyiceberg.catalog.sql import SqlCatalog
        
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", getattr(tigris, "S3_ACCESS_KEY", ""))
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", getattr(tigris, "S3_SECRET_KEY", ""))
        s3_endpoint = getattr(tigris, "TIGRIS_ENDPOINT", "https://fly.storage.tigris.dev")
        s3_endpoint = os.environ.get("AWS_ENDPOINT_URL_S3", s3_endpoint)
        
        # PyIceberg expects s3.endpoint to NOT be used if it's hitting AWS directly,
        # but since we are specifically using Tigris/Minio S3 compatible storage:
        return SqlCatalog(
            "duckpond",
            **{
                "uri": f"sqlite:///{PYICEBERG_DB_PATH}",
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": aws_access_key_id,
                "s3.secret-access-key": aws_secret_access_key,
                "s3.region": os.environ.get("AWS_REGION", "auto")
            }
        )
    except Exception as e:
        log.warning(f"Failed to initialize PyIceberg catalog: {e}")
        return None

def init_iceberg_catalog():
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS iceberg_tables (
                namespace TEXT,
                name TEXT,
                metadata_location TEXT,
                schema_json TEXT,
                PRIMARY KEY (namespace, name)
            )
        ''')
        
        try:
            conn.execute("ALTER TABLE iceberg_tables ADD COLUMN type TEXT DEFAULT 'table'")
        except sqlite3.OperationalError:
            pass # Column already exists
            
        # Organization tables
        conn.execute('''
            CREATE TABLE IF NOT EXISTS organizations (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                slug TEXT UNIQUE NOT NULL,
                created_by TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS org_members (
                org_id TEXT,
                user_email TEXT NOT NULL COLLATE NOCASE,
                user_id TEXT,
                role TEXT DEFAULT 'member',
                invited_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (org_id, user_email)
            )
        ''')
        
        # Query History / Audit Log
        conn.execute('''
            CREATE TABLE IF NOT EXISTS query_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL,
                user_email TEXT,
                sql_text TEXT NOT NULL,
                status TEXT DEFAULT 'running',
                error_msg TEXT,
                duration_ms INTEGER,
                created_at TEXT DEFAULT (datetime('now'))
            )
        ''')
        # Purge corrupted entries from prior buggy code that stored raw request bodies
        rows = conn.execute("SELECT namespace, name, schema_json FROM iceberg_tables").fetchall()
        for ns, tbl, schema_str in rows:
            try:
                meta = json.loads(schema_str)
                if "format-version" not in meta:
                    log.warning(f"Purging corrupted catalog entry: {ns}.{tbl} (missing format-version)")
                    conn.execute("DELETE FROM iceberg_tables WHERE namespace=? AND name=?", (ns, tbl))
            except Exception:
                log.warning(f"Purging unparseable catalog entry: {ns}.{tbl}")
                conn.execute("DELETE FROM iceberg_tables WHERE namespace=? AND name=?", (ns, tbl))
init_iceberg_catalog()

# -- Catalog Backup & Restore to S3 -------------------------------------------
CATALOG_BACKUP_KEY = "backups/catalog.db"
CATALOG_BACKUP_INTERVAL = 300  # 5 minutes

def restore_catalog_from_s3():
    """On startup, if the local catalog.db has no tables/orgs, attempt to restore from S3."""
    try:
        with sqlite3.connect(CATALOG_DB_PATH) as conn:
            table_count = conn.execute("SELECT COUNT(*) FROM iceberg_tables").fetchone()[0]
            org_count = conn.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
        
        if table_count > 0 or org_count > 0:
            log.info(f"Catalog has {table_count} tables, {org_count} orgs — skipping S3 restore.")
            return
        
        log.info("Catalog is empty — attempting restore from S3 backup...")
        s3 = tigris.get_s3_client()
        
        import tempfile
        tmp_path = CATALOG_DB_PATH + ".restore"
        s3.download_file(tigris.BUCKET_NAME, CATALOG_BACKUP_KEY, tmp_path)
        
        # Validate the downloaded file is a valid SQLite database
        with sqlite3.connect(tmp_path) as test_conn:
            restored_tables = test_conn.execute("SELECT COUNT(*) FROM iceberg_tables").fetchone()[0]
            restored_orgs = test_conn.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
        
        if restored_tables > 0 or restored_orgs > 0:
            import shutil
            shutil.copy2(tmp_path, CATALOG_DB_PATH)
            log.info(f"✅ Catalog restored from S3! ({restored_tables} tables, {restored_orgs} orgs)")
        else:
            log.info("S3 backup exists but is empty — starting fresh.")
        
        try:
            os.remove(tmp_path)
        except:
            pass
            
    except Exception as e:
        log.info(f"No S3 backup found or restore failed (this is normal on first run): {e}")

def backup_catalog_to_s3():
    """Upload a copy of catalog.db to S3."""
    try:
        # Use SQLite backup API for a consistent snapshot
        import shutil
        backup_path = CATALOG_DB_PATH + ".backup"
        
        src = sqlite3.connect(CATALOG_DB_PATH)
        dst = sqlite3.connect(backup_path)
        src.backup(dst)
        src.close()
        dst.close()
        
        s3 = tigris.get_s3_client()
        s3.upload_file(backup_path, tigris.BUCKET_NAME, CATALOG_BACKUP_KEY)
        
        # Also keep a timestamped version (last N rotations)
        from datetime import datetime
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        s3.upload_file(backup_path, tigris.BUCKET_NAME, f"backups/catalog_{ts}.db")
        
        try:
            os.remove(backup_path)
        except:
            pass
        
        with sqlite3.connect(CATALOG_DB_PATH) as conn:
            table_count = conn.execute("SELECT COUNT(*) FROM iceberg_tables").fetchone()[0]
            org_count = conn.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
        
        log.info(f"✅ Catalog backed up to S3 ({table_count} tables, {org_count} orgs)")
    except Exception as e:
        log.error(f"Catalog backup failed: {e}")

def _catalog_backup_loop():
    """Background thread: backup catalog to S3 periodically."""
    import time as _time
    _time.sleep(60)  # Wait 60s after startup before first backup
    while True:
        try:
            backup_catalog_to_s3()
        except Exception as e:
            log.error(f"Backup loop error: {e}")
        _time.sleep(CATALOG_BACKUP_INTERVAL)

# Run restore on startup, then start background backup thread
restore_catalog_from_s3()

import threading
_backup_thread = threading.Thread(target=_catalog_backup_loop, daemon=True)
_backup_thread.start()
log.info(f"Catalog backup thread started (interval={CATALOG_BACKUP_INTERVAL}s)")
# ------------------------------------------------------------------------------

# -- Rate Limiting & Query Timeout ---------------------------------------------
import threading

MAX_CONCURRENT_QUERIES_PER_TENANT = int(os.environ.get("MAX_CONCURRENT_QUERIES", "10"))
QUERY_TIMEOUT_SECONDS = int(os.environ.get("QUERY_TIMEOUT_SECONDS", "120"))

class TenantRateLimiter:
    """Thread-safe per-tenant concurrent query limiter."""
    def __init__(self, max_concurrent: int):
        self._max = max_concurrent
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()
    
    def acquire(self, tenant_ns: str) -> bool:
        with self._lock:
            current = self._counts.get(tenant_ns, 0)
            if current >= self._max:
                return False
            self._counts[tenant_ns] = current + 1
            return True
    
    def release(self, tenant_ns: str):
        with self._lock:
            current = self._counts.get(tenant_ns, 0)
            self._counts[tenant_ns] = max(0, current - 1)
    
    def current(self, tenant_ns: str) -> int:
        with self._lock:
            return self._counts.get(tenant_ns, 0)

_rate_limiter = TenantRateLimiter(MAX_CONCURRENT_QUERIES_PER_TENANT)
# ------------------------------------------------------------------------------

# -- Fly/Tigris Setup ----------------------------------------------------------
def _table_to_response(table: pa.Table, limit: int) -> dict:
    sliced  = table.slice(0, limit)
    columns = sliced.schema.names
    pydict  = sliced.to_pydict()
    rows    = [{col: pydict[col][i] for col in columns} for i in range(sliced.num_rows)]
    return {"columns": columns, "rows": rows, "row_count": sliced.num_rows}

# -- API -----------------------------------------------------------------------
@app.get("/status")
def status():
    # Basic health + Tigris connectivity check
    try:
        files, _ = tigris.list_table_files("data/")
        return {"status": "ok", "message": f"OK — synthetic files in {tigris.BUCKET_NAME}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/backup")
async def trigger_backup(user: dict = Depends(require_auth)):
    """Manually trigger a catalog backup to S3. Requires auth."""
    try:
        backup_catalog_to_s3()
        return {"status": "ok", "message": "Catalog backed up to S3 successfully."}
    except Exception as e:
        return {"status": "error", "message": f"Backup failed: {str(e)}"}

@app.get("/query-history")
async def query_history(
    limit: int = 100,
    status: Optional[str] = None,
    user: dict = Depends(require_auth)
):
    """Return recent query history for the authenticated user's namespace."""
    tenant_ns = user.get("namespace", "duckpond")
    
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        if status:
            rows = conn.execute(
                "SELECT id, namespace, user_email, sql_text, status, error_msg, duration_ms, created_at FROM query_history WHERE namespace=? AND status=? ORDER BY id DESC LIMIT ?",
                (tenant_ns, status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, namespace, user_email, sql_text, status, error_msg, duration_ms, created_at FROM query_history WHERE namespace=? ORDER BY id DESC LIMIT ?",
                (tenant_ns, limit)
            ).fetchall()
    
    return {"queries": [dict(r) for r in rows]}

# -- Organization Management ---------------------------------------------------
class CreateOrgRequest(BaseModel):
    name: str
    slug: str

class InviteMemberRequest(BaseModel):
    email: str
    role: str = "member"

@app.post("/orgs")
async def create_org(req: CreateOrgRequest, user: dict = Depends(require_auth)):
    """Create a new organization. Creator becomes admin."""
    import re
    slug = re.sub(r'[^a-z0-9_]', '', req.slug.lower().strip())
    if not slug or len(slug) < 2:
        raise HTTPException(status_code=400, detail="Slug must be at least 2 lowercase alphanumeric characters")
    
    org_id = str(uuid.uuid4())
    email = user.get("email", "")
    user_id = user.get("id", "")
    
    try:
        with sqlite3.connect(CATALOG_DB_PATH) as conn:
            conn.execute(
                "INSERT INTO organizations (id, name, slug, created_by) VALUES (?, ?, ?, ?)",
                (org_id, req.name.strip(), slug, user_id)
            )
            conn.execute(
                "INSERT INTO org_members (org_id, user_email, user_id, role) VALUES (?, ?, ?, 'admin')",
                (org_id, email, user_id)
            )
        log.info(f"Org created: {req.name} (slug={slug}) by {email}")
        return {"id": org_id, "name": req.name, "slug": slug, "namespace": f"org_{slug}"}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=409, detail=f"Organization slug '{slug}' already exists")

@app.get("/orgs")
async def list_orgs(user: dict = Depends(require_auth)):
    """List organizations the current user belongs to."""
    email = user.get("email", "")
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        rows = conn.execute('''
            SELECT o.id, o.name, o.slug, m.role
            FROM organizations o JOIN org_members m ON o.id = m.org_id
            WHERE m.user_email = ?
        ''', (email,)).fetchall()
    return {"orgs": [{"id": r[0], "name": r[1], "slug": r[2], "role": r[3]} for r in rows]}

@app.get("/orgs/{slug}/members")
async def list_members(slug: str, user: dict = Depends(require_auth)):
    """List members of an organization."""
    email = user.get("email", "")
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        # Verify user is a member
        org_row = conn.execute(
            "SELECT o.id FROM organizations o JOIN org_members m ON o.id = m.org_id WHERE o.slug = ? AND m.user_email = ?",
            (slug, email)
        ).fetchone()
        if not org_row:
            raise HTTPException(status_code=403, detail="Not a member of this organization")
        
        members = conn.execute(
            "SELECT user_email, role, invited_at FROM org_members WHERE org_id = ?",
            (org_row[0],)
        ).fetchall()
    return {"members": [{"email": m[0], "role": m[1], "invited_at": m[2]} for m in members]}

@app.post("/orgs/{slug}/members")
async def invite_member(slug: str, req: InviteMemberRequest, user: dict = Depends(require_auth)):
    """Invite a member to an organization. Only admins can invite."""
    email = user.get("email", "")
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        # Verify user is admin
        org_row = conn.execute(
            "SELECT o.id FROM organizations o JOIN org_members m ON o.id = m.org_id WHERE o.slug = ? AND m.user_email = ? AND m.role = 'admin'",
            (slug, email)
        ).fetchone()
        if not org_row:
            raise HTTPException(status_code=403, detail="Only admins can invite members")
        
        try:
            conn.execute(
                "INSERT INTO org_members (org_id, user_email, role) VALUES (?, ?, ?)",
                (org_row[0], req.email.strip().lower(), req.role)
            )
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail=f"{req.email} is already a member")
    
    log.info(f"Invited {req.email} to org {slug} as {req.role}")
    return {"status": f"{req.email} invited as {req.role}"}

@app.delete("/orgs/{slug}/members/{member_email}")
async def remove_member(slug: str, member_email: str, user: dict = Depends(require_auth)):
    """Remove a member from an organization. Admins can remove others, anyone can leave."""
    email = user.get("email", "")
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        org_row = conn.execute("SELECT id FROM organizations WHERE slug = ?", (slug,)).fetchone()
        if not org_row:
            raise HTTPException(status_code=404, detail="Organization not found")
        
        # Check if user is admin or removing themselves
        if member_email.lower() != email.lower():
            admin_check = conn.execute(
                "SELECT 1 FROM org_members WHERE org_id = ? AND user_email = ? AND role = 'admin'",
                (org_row[0], email)
            ).fetchone()
            if not admin_check:
                raise HTTPException(status_code=403, detail="Only admins can remove other members")
        
        conn.execute(
            "DELETE FROM org_members WHERE org_id = ? AND user_email = ?",
            (org_row[0], member_email.lower())
        )
    log.info(f"Removed {member_email} from org {slug}")
    return {"status": f"{member_email} removed from {slug}"}


def _delete_s3_namespace(namespace: str):
    """Delete all S3 objects for a namespace across all known prefixes."""
    s3 = tigris.get_s3_client()
    total_deleted = 0
    for prefix in [f"iceberg/{namespace}/", f"uploads/{namespace}/", f"data/{namespace}/", f"staging/{namespace}/"]:
        while True:
            res = s3.list_objects_v2(Bucket=tigris.BUCKET_NAME, Prefix=prefix, MaxKeys=1000)
            objects = res.get("Contents", [])
            if not objects:
                break
            s3.delete_objects(Bucket=tigris.BUCKET_NAME, Delete={"Objects": [{"Key": o["Key"]} for o in objects]})
            total_deleted += len(objects)
            if not res.get("IsTruncated"):
                break
    if total_deleted:
        log.info(f"S3 cleanup: deleted {total_deleted} objects for namespace '{namespace}'")
    return total_deleted


@app.delete("/orgs/{slug}")
async def delete_org(slug: str, user: dict = Depends(require_auth)):
    """Delete an organization. Admin only. Removes org, all members, and all Iceberg tables in the org namespace."""
    email = user.get("email", "")
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        org_row = conn.execute(
            "SELECT o.id FROM organizations o JOIN org_members m ON o.id = m.org_id WHERE o.slug = ? AND m.user_email = ? AND m.role = 'admin'",
            (slug, email)
        ).fetchone()
        if not org_row:
            raise HTTPException(status_code=403, detail="Only admins can delete an organization")
        
        org_ns = f"org_{slug}"
        
        # Delete all Iceberg tables in the org namespace
        tables = conn.execute("SELECT name FROM iceberg_tables WHERE namespace = ?", (org_ns,)).fetchall()
        for (tbl_name,) in tables:
            conn.execute("DELETE FROM iceberg_tables WHERE namespace = ? AND name = ?", (org_ns, tbl_name))
            log.info(f"Deleted Iceberg table {org_ns}.{tbl_name}")
        
        # Clean up S3 data for the org
        try:
            _delete_s3_namespace(org_ns)
        except Exception as e:
            log.warning(f"S3 cleanup warning (non-critical): {e}")
        
        # Delete org members and org
        conn.execute("DELETE FROM org_members WHERE org_id = ?", (org_row[0],))
        conn.execute("DELETE FROM organizations WHERE id = ?", (org_row[0],))
    
    log.info(f"Org deleted: {slug} by {email}")
    return {"status": f"Organization '{slug}' and all its data deleted"}

@app.delete("/account")
async def delete_account(user: dict = Depends(require_auth)):
    """Full account deletion: Stripe, org memberships, Tigris data, billing profile, Supabase auth."""
    email   = user.get("email", "")
    user_id = user.get("id", "")
    ns      = auth_module.get_tenant_namespace(user)
    steps   = []

    # ── 1. Stripe: find customer by email, cancel subscriptions, delete ────────
    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    if stripe.api_key and not stripe.api_key.startswith("sk_test_mock"):
        try:
            customers = stripe.Customer.list(email=email, limit=5)
            for cust in customers.auto_paging_iter():
                subs = stripe.Subscription.list(customer=cust.id, status="active", limit=20)
                for sub in subs.auto_paging_iter():
                    stripe.Subscription.cancel(sub.id)
                    steps.append(f"cancelled stripe subscription {sub.id}")
                stripe.Customer.delete(cust.id)
                steps.append(f"deleted stripe customer {cust.id}")
        except Exception as e:
            log.warning(f"Stripe cleanup warning: {e}")
            steps.append(f"stripe warning: {e}")
    else:
        steps.append("stripe skipped (test mode or no key)")

    # ── 2. Org memberships: remove user; delete org if sole admin ──────────────
    orgs_to_delete = []
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        admin_orgs = conn.execute(
            "SELECT o.id, o.slug FROM organizations o "
            "JOIN org_members m ON o.id = m.org_id "
            "WHERE m.user_email = ? AND m.role = 'admin'",
            (email,)
        ).fetchall()

        for org_id, slug in admin_orgs:
            other_admins = conn.execute(
                "SELECT COUNT(*) FROM org_members WHERE org_id = ? AND role = 'admin' AND user_email != ?",
                (org_id, email)
            ).fetchone()[0]
            if other_admins == 0:
                orgs_to_delete.append((org_id, slug))
            else:
                conn.execute("DELETE FROM org_members WHERE org_id = ? AND user_email = ?", (org_id, email))
                steps.append(f"removed from org '{slug}' (other admins remain)")

        # Remove all remaining non-admin memberships
        exclude = ",".join("?" * len(orgs_to_delete)) if orgs_to_delete else "NULL"
        conn.execute(
            f"DELETE FROM org_members WHERE user_email = ? AND org_id NOT IN ({exclude})",
            [email] + [o[0] for o in orgs_to_delete]
        )

        for org_id, slug in orgs_to_delete:
            org_ns = f"org_{slug}"
            tables = conn.execute("SELECT name FROM iceberg_tables WHERE namespace = ?", (org_ns,)).fetchall()
            for (tbl_name,) in tables:
                conn.execute("DELETE FROM iceberg_tables WHERE namespace = ? AND name = ?", (org_ns, tbl_name))
            conn.execute("DELETE FROM org_members WHERE org_id = ?", (org_id,))
            conn.execute("DELETE FROM organizations WHERE id = ?", (org_id,))
            try:
                deleted = _delete_s3_namespace(org_ns)
                steps.append(f"deleted org '{slug}' + {deleted} S3 objects")
            except Exception as e:
                log.warning(f"Org S3 cleanup warning ({slug}): {e}")

        tables = conn.execute("SELECT name FROM iceberg_tables WHERE namespace = ?", (ns,)).fetchall()
        for (tbl_name,) in tables:
            conn.execute("DELETE FROM iceberg_tables WHERE namespace = ? AND name = ?", (ns, tbl_name))

    # ── 3. Tigris: wipe personal namespace ────────────────────────────────────
    try:
        deleted = _delete_s3_namespace(ns)
        steps.append(f"deleted {deleted} personal S3 objects")
    except Exception as e:
        log.warning(f"Personal S3 cleanup warning: {e}")

    # ── 4. Billing profile ─────────────────────────────────────────────────────
    try:
        auth_module.supabase.table("billing_profiles").delete().eq("owner_id", user_id).execute()
        steps.append("billing profile deleted")
    except Exception as e:
        log.warning(f"Billing profile cleanup failed: {e}")

    # ── 5. Supabase auth user ─────────────────────────────────────────────────
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    if service_key and user_id:
        try:
            req = urllib.request.Request(
                f"{auth_module.SUPABASE_URL}/auth/v1/admin/users/{user_id}",
                method="DELETE"
            )
            req.add_header("Authorization", f"Bearer {service_key}")
            req.add_header("apikey", auth_module.SUPABASE_ANON_KEY)
            urllib.request.urlopen(req, timeout=10)
            steps.append("supabase auth user deleted")
        except Exception as e:
            log.warning(f"Supabase auth deletion failed: {e}")
            steps.append(f"supabase warning: {e}")

    log.info(f"Account fully deleted: {email} | steps: {steps}")
    return {"status": "deleted", "email": email, "steps": steps}


@app.get("/lakehouses")
def lakehouses(user: dict = Depends(require_auth)):
    ns = user["namespace"]
    try:
        with sqlite3.connect(CATALOG_DB_PATH) as conn:
            rows = conn.execute("SELECT name FROM iceberg_tables WHERE namespace=? AND name IS NOT NULL", (ns,)).fetchall()
        return {"tables": [{"name": r[0]} for r in rows]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/schema")
def get_schema(user: dict = Depends(require_auth)):
    ns = user["namespace"]
    try:
        tables = []
        with sqlite3.connect(CATALOG_DB_PATH) as conn:
            rows = conn.execute("SELECT name, schema_json, type FROM iceberg_tables WHERE namespace=? AND name IS NOT NULL", (ns,)).fetchall()
            
            for t_name, sch_json_str, t_type in rows:
                cols = []
                metrics = {}
                history = []
                properties = {}
                
                if sch_json_str:
                    try:
                        meta = json.loads(sch_json_str)
                        schemas = meta.get("schemas", [])
                        current_id = meta.get("current-schema-id", 0)
                        schema = next((s for s in schemas if s.get("schema-id") == current_id), schemas[0] if schemas else {})
                        for f in schema.get("fields", []):
                            cols.append({"name": f.get("name"), "type": str(f.get("type"))})
                            
                        # Extract Snapshots for History
                        for snap in meta.get("snapshots", []):
                            history.append({
                                "timestamp_ms": snap.get("timestamp-ms"),
                                "operation": snap.get("summary", {}).get("operation"),
                                "summary": snap.get("summary", {})
                            })
                            
                        # Extract Metrics from the latest snapshot
                        latest_snap = meta.get("snapshots", [])[-1] if meta.get("snapshots") else {}
                        summary = latest_snap.get("summary", {})
                        
                        total_rows = int(summary.get("total-records", 0))
                        total_size = int(summary.get("total-file-size", 0))
                        total_files = int(summary.get("total-data-files", 0))
                        
                        # Fallback to authentic Tigris scan if DuckDB omitted metrics
                        if total_size == 0 and meta.get("location"):
                            loc = meta.get("location", "")
                            if loc.startswith("s3://"):
                                parts = loc.replace("s3://", "").split("/")
                                prefix = "/".join(parts[1:]) + "/data/"
                                t_size, t_files = tigris.get_directory_size(prefix)
                                total_size = t_size
                                if total_files == 0: total_files = t_files

                        metrics = {
                            "total_rows": total_rows,
                            "total_size_bytes": total_size,
                            "total_data_files": total_files
                        }
                        
                        properties = meta.get("properties", {})
                    except Exception as e: 
                        log.warning(f"Failed to parse enhanced metadata for {t_name}: {e}")
                        
                tables.append({
                    "name": t_name, 
                    "type": t_type,
                    "columns": cols, 
                    "metrics": metrics, 
                    "history": history, 
                    "properties": properties
                })
        return {"tables": tables, "namespace": ns}
    except Exception as e:
        return {"error": str(e)}

# -- dbt Runner ----------------------------------------------------------------
import subprocess
import tempfile
import shutil
import zipfile
import threading

# In-memory job store (coordinator is single-instance)
_dbt_jobs: dict[str, dict] = {}

class DbtRunRequest(BaseModel):
    command: str = "build"  # build, run, test, compile, seed
    git_url: str = ""       # optional: clone from git
    select: str = ""        # optional: --select flag
    full_refresh: bool = False

def _generate_profiles_yml(namespace: str, profile_name: str = "jaffle_shop", db_path: str = ":memory:") -> str:
    """Generate a profiles.yml that configures dbt-duckdb to write locally (fast seeds)."""
    endpoint = os.environ.get('AWS_ENDPOINT_URL_S3', 'https://fly.storage.tigris.dev').replace('https://', '').replace('http://', '')
    access_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    
    return f"""{profile_name}:
  target: duckpond
  outputs:
    duckpond:
      type: duckdb
      path: '{db_path}'
      threads: 4
      extensions:
        - httpfs
        - parquet
      settings:
        s3_endpoint: '{endpoint}'
        s3_access_key_id: '{access_key}'
        s3_secret_access_key: '{secret_key}'
        s3_use_ssl: true
        s3_url_style: vhost
        s3_region: us-east-1
"""

def _read_profile_name(project_dir: str) -> str:
    """Read the profile name from dbt_project.yml."""
    try:
        dbt_proj_path = os.path.join(project_dir, "dbt_project.yml")
        with open(dbt_proj_path) as f:
            for line in f:
                if line.strip().startswith("profile:"):
                    val = line.split(":", 1)[1].strip().strip("'\"")
                    # Strip YAML comments
                    if "#" in val:
                        val = val[:val.index("#")].strip().strip("'\"")
                    return val
    except: pass
    return "jaffle_shop"

def _run_dbt_job(job_id: str, project_dir: str, namespace: str, command: str, select: str, full_refresh: bool):
    """Run dbt in a subprocess and capture output."""
    job = _dbt_jobs[job_id]
    job["status"] = "running"
    job["started_at"] = time.time()
    
    try:
        # Read profile name from project and write profiles.yml
        profile_name = _read_profile_name(project_dir)
        db_path = os.path.join(project_dir, "dbt_run.duckdb")
        profiles_path = os.path.join(project_dir, "profiles.yml")
        with open(profiles_path, "w") as f:
            f.write(_generate_profiles_yml(namespace, profile_name, db_path))
        # Install dbt package dependencies if needed
        packages_file = os.path.join(project_dir, "packages.yml")
        if not os.path.exists(packages_file):
            packages_file = os.path.join(project_dir, "dependencies.yml")
        if os.path.exists(packages_file):
            log.info(f"dbt job {job_id}: installing dependencies...")
            deps_result = subprocess.run(
                ["dbt", "deps", "--profiles-dir", project_dir, "--project-dir", project_dir],
                capture_output=True, text=True, timeout=120,
                cwd=project_dir, env={**os.environ, "DBT_PROFILES_DIR": project_dir}
            )
            if deps_result.returncode != 0:
                job["status"] = "failed"
                job["logs"] = deps_result.stdout.splitlines() + deps_result.stderr.splitlines()
                job["finished_at"] = time.time()
                job["duration"] = round(job["finished_at"] - job["started_at"], 1)
                log.warning(f"dbt deps failed: {deps_result.stderr[:200]}")
                return
        
        # Build the --vars flag if needed
        vars_flag = []
        dbt_proj_path = os.path.join(project_dir, "dbt_project.yml")
        try:
            with open(dbt_proj_path) as f:
                proj_content = f.read()
            if "load_source_data" in proj_content:
                vars_flag = ["--vars", '{"load_source_data": true}']
                log.info(f"dbt job {job_id}: auto-enabled load_source_data var")
        except: pass
        
        # Pre-run seeds before build/run to ensure source tables exist
        # (dbt sources aren't connected to seeds in the DAG)
        seeds_dir = os.path.join(project_dir, "seeds")
        if command in ("build", "run") and os.path.isdir(seeds_dir) and os.listdir(seeds_dir):
            seed_cmd = ["dbt", "seed", "--profiles-dir", project_dir, "--project-dir", project_dir] + vars_flag
            log.info(f"dbt job {job_id}: pre-seeding: {' '.join(seed_cmd)}")
            seed_result = subprocess.run(
                seed_cmd, capture_output=True, text=True, timeout=120,
                cwd=project_dir, env={**os.environ, "DBT_PROFILES_DIR": project_dir}
            )
            if seed_result.returncode != 0:
                job["status"] = "failed"
                job["logs"] = ["Pre-seed failed:"] + seed_result.stdout.splitlines() + seed_result.stderr.splitlines()
                job["finished_at"] = time.time()
                job["duration"] = round(job["finished_at"] - job["started_at"], 1)
                log.warning(f"dbt seed failed for job {job_id}")
                return
            job["logs"] = seed_result.stdout.splitlines()
        
        # Build main dbt command
        cmd = ["dbt", command, "--profiles-dir", project_dir, "--project-dir", project_dir]
        if select:
            cmd.extend(["--select", select])
        if full_refresh and command in ("build", "run"):
            cmd.append("--full-refresh")
        cmd.extend(vars_flag)
        
        log.info(f"dbt job {job_id}: {' '.join(cmd)}")
        
        # Run with timeout
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=project_dir,
            env={**os.environ, "DBT_PROFILES_DIR": project_dir}
        )
        job["pid"] = proc.pid
        
        lines = []
        deadline = time.time() + 600  # 10 minute timeout
        
        for line in iter(proc.stdout.readline, ''):
            if time.time() > deadline:
                proc.kill()
                lines.append("\n⚠️ dbt job timed out after 10 minutes")
                job["status"] = "timeout"
                break
            lines.append(line.rstrip())
            job["logs"] = lines[-200:]  # Keep last 200 lines
        
        proc.wait()
        
        if job["status"] != "timeout":
            job["status"] = "success" if proc.returncode == 0 else "failed"
        job["exit_code"] = proc.returncode
        job["logs"] = lines
        job["finished_at"] = time.time()
        job["duration"] = round(job["finished_at"] - job["started_at"], 1)
        
        # If successful, register dbt output tables in Iceberg catalog
        if job["status"] == "success" and command in ("build", "run", "seed"):
            _register_dbt_tables(project_dir, namespace, job_id)
        
        log.info(f"dbt job {job_id}: {job['status']} ({job['duration']}s)")
        
    except Exception as e:
        job["status"] = "error"
        job["logs"] = job.get("logs", []) + [f"Internal error: {str(e)}"]
        job["finished_at"] = time.time()
        log.exception(f"dbt job {job_id} failed")
    finally:
        # Clean up temp directory
        try:
            if project_dir.startswith(tempfile.gettempdir()):
                shutil.rmtree(project_dir, ignore_errors=True)
        except: pass

def _register_dbt_tables(project_dir: str, namespace: str, job_id: str):
    """After a dbt run, export tables to S3, then route through a worker for proper Iceberg creation."""
    db_path = os.path.join(project_dir, "dbt_run.duckdb")
    bucket = os.environ.get('BUCKET_NAME', 'duckpond-data')
    
    if not os.path.exists(db_path):
        log.warning(f"dbt job {job_id}: DuckDB file not found at {db_path}")
        return
    
    try:
        # 1. Get list of materialized tables
        src = duckdb.connect(db_path, read_only=True)
        tables = src.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE' 
            AND table_schema = 'main'
            AND table_name NOT LIKE 'raw_%'
        """).fetchall()
        src.close()
        
        if not tables:
            log.info(f"dbt job {job_id}: no tables to register")
            return
        
        # 2. Export each table to S3 as a staging parquet file
        con = duckdb.connect(db_path)
        endpoint = os.environ.get('AWS_ENDPOINT_URL_S3', 'https://fly.storage.tigris.dev').replace('https://', '').replace('http://', '')
        con.execute(f"SET s3_endpoint='{endpoint}'")
        con.execute(f"SET s3_access_key_id='{os.environ.get('AWS_ACCESS_KEY_ID', '')}'")
        con.execute(f"SET s3_secret_access_key='{os.environ.get('AWS_SECRET_ACCESS_KEY', '')}'")
        con.execute("SET s3_use_ssl=true")
        con.execute("SET s3_url_style='vhost'")
        con.execute("SET s3_region='us-east-1'")
        
        staged_tables = []
        for schema_name, table_name in tables:
            try:
                staging_key = f"staging/dbt_{job_id}_{table_name}.parquet"
                s3_path = f"s3://{bucket}/{staging_key}"
                con.execute(f'COPY (SELECT * FROM "{schema_name}"."{table_name}") TO \'{s3_path}\' (FORMAT \'parquet\', COMPRESSION \'zstd\', ROW_GROUP_SIZE 50000)')
                staged_tables.append((table_name, s3_path))
                log.info(f"dbt job {job_id}: staged {table_name} → {s3_path}")
            except Exception as e:
                log.warning(f"dbt job {job_id}: failed to stage {table_name}: {e}")
        con.close()
        
        # 3. Route through worker for Iceberg creation (same path as data loader)
        tig_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
        tig_secret = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
        tig_ep = os.environ.get('AWS_ENDPOINT_URL_S3', 'https://fly.storage.tigris.dev').replace('https://', '').replace('http://', '')
        
        iceberg_binds = f"""
        CREATE SECRET IF NOT EXISTS tigris_s3 (TYPE S3, KEY_ID '{tig_key}', SECRET '{tig_secret}', ENDPOINT '{tig_ep}');
        CREATE SECRET IF NOT EXISTS iceberg_auth (TYPE ICEBERG, TOKEN '{os.environ.get("INTERNAL_AUTH_TOKEN", "duckpond-internal")}');
        ATTACH IF NOT EXISTS 'duckpond' AS enterprise_lake (
            TYPE ICEBERG,
            ENDPOINT 'https://duckpond-coordinator.fly.dev'
        );
        """
        
        tables_registered = []
        app_name = os.environ.get('WORKER_HOST', 'duckpond-worker.internal').split('.')[0]
        worker_host = f"{app_name}.internal:8080"
        
        # Wake workers first
        if not os.environ.get('LOCAL_MODE'):
            try:
                wake_workers()
                time.sleep(2)
            except: pass
        
        for table_name, staged_path in staged_tables:
            try:
                sql = f"{iceberg_binds}\nDROP TABLE IF EXISTS enterprise_lake.{namespace}.{table_name};\nCREATE TABLE enterprise_lake.{namespace}.{table_name} AS SELECT * FROM read_parquet('{staged_path}');"
                
                # Call worker via Arrow Flight
                for attempt in range(10):
                    try:
                        client = flight.FlightClient(f"grpc://{worker_host}")
                        ticket = flight.Ticket(json.dumps({
                            "sql": sql,
                            "partition_id": "0",
                            "output_mode": "flight_stream",
                            "job_id": f"dbt_{job_id}_{table_name}"
                        }).encode('utf-8'))
                        client.do_get(ticket).read_all()
                        break
                    except Exception as e:
                        if attempt == 9:
                            raise Exception(f"Worker call failed after 10 attempts: {e}")
                        time.sleep(1.0)
                
                tables_registered.append(table_name)
                log.info(f"dbt job {job_id}: ✓ {table_name} registered via Iceberg catalog")
                
                # Clean up staging file
                try:
                    s3 = tigris.get_s3_client()
                    staging_key = f"staging/dbt_{job_id}_{table_name}.parquet"
                    s3.delete_object(Bucket=bucket, Key=staging_key)
                except: pass
                
            except Exception as e:
                log.warning(f"dbt job {job_id}: Iceberg registration failed for {table_name}: {e}")
        
        if tables_registered:
            _dbt_jobs[job_id]["tables"] = tables_registered
            log.info(f"dbt job {job_id}: registered {len(tables_registered)} Iceberg tables: {tables_registered}")
    except Exception as e:
        log.warning(f"dbt table registration error: {e}")

@app.post("/dbt/run")
async def dbt_run(req: DbtRunRequest, user: dict = Depends(require_auth)):
    """Execute a dbt command against the user's namespace."""
    ns = user["namespace"]
    job_id = str(uuid.uuid4())[:8]
    
    # Prepare project directory
    project_dir = None
    
    if req.git_url:
        # Clone from git
        project_dir = os.path.join(tempfile.gettempdir(), f"dbt_{job_id}")
        try:
            result = subprocess.run(
                ["git", "clone", "--depth", "1", req.git_url, project_dir],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                return {"error": f"Git clone failed: {result.stderr}"}
        except subprocess.TimeoutExpired:
            return {"error": "Git clone timed out (30s)"}
    else:
        # Use the built-in jaffle_shop project
        builtin_path = "/app/dbt-project" if os.path.exists("/app/dbt-project") else os.path.join(os.path.dirname(__file__), "..", "dbt-project")
        if os.path.exists(builtin_path):
            project_dir = os.path.join(tempfile.gettempdir(), f"dbt_{job_id}")
            shutil.copytree(builtin_path, project_dir, ignore=shutil.ignore_patterns('.git', 'target', 'logs', '__pycache__'))
        else:
            return {"error": "No dbt project available. Upload a project or provide a git URL."}
    
    # Initialize job
    _dbt_jobs[job_id] = {
        "id": job_id,
        "status": "queued",
        "command": req.command,
        "namespace": ns,
        "user": user.get("email", ""),
        "logs": [],
        "tables": [],
        "created_at": time.time(),
    }
    
    # Run in background thread
    thread = threading.Thread(
        target=_run_dbt_job,
        args=(job_id, project_dir, ns, req.command, req.select, req.full_refresh),
        daemon=True
    )
    thread.start()
    
    return {"job_id": job_id, "status": "queued", "message": f"dbt {req.command} started"}

@app.post("/dbt/upload")
async def dbt_upload(
    file: UploadFile = File(...),
    command: str = Form("build"),
    user: dict = Depends(require_auth)
):
    """Upload a dbt project as a zip file and run it."""
    ns = user["namespace"]
    job_id = str(uuid.uuid4())[:8]
    
    # Save and extract zip
    project_dir = os.path.join(tempfile.gettempdir(), f"dbt_{job_id}")
    os.makedirs(project_dir, exist_ok=True)
    
    try:
        content = await file.read()
        zip_path = os.path.join(project_dir, "project.zip")
        with open(zip_path, "wb") as f:
            f.write(content)
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(project_dir)
        os.remove(zip_path)
        
        # If the zip contained a single directory, use that as project root
        entries = [e for e in os.listdir(project_dir) if not e.startswith('.')]
        if len(entries) == 1 and os.path.isdir(os.path.join(project_dir, entries[0])):
            inner = os.path.join(project_dir, entries[0])
            if os.path.exists(os.path.join(inner, "dbt_project.yml")):
                project_dir = inner
        
        if not os.path.exists(os.path.join(project_dir, "dbt_project.yml")):
            return {"error": "No dbt_project.yml found in uploaded zip"}
        
    except zipfile.BadZipFile:
        return {"error": "Invalid zip file"}
    
    # Initialize job
    _dbt_jobs[job_id] = {
        "id": job_id,
        "status": "queued",
        "command": command,
        "namespace": ns,
        "user": user.get("email", ""),
        "logs": [],
        "tables": [],
        "created_at": time.time(),
    }
    
    thread = threading.Thread(
        target=_run_dbt_job,
        args=(job_id, project_dir, ns, command, "", False),
        daemon=True
    )
    thread.start()
    
    return {"job_id": job_id, "status": "queued", "message": f"dbt {command} started from uploaded project"}

@app.get("/dbt/status/{job_id}")
async def dbt_status(job_id: str, user: dict = Depends(require_auth)):
    """Get the status of a dbt job."""
    job = _dbt_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "id": job["id"],
        "status": job["status"],
        "command": job["command"],
        "duration": job.get("duration"),
        "tables": job.get("tables", []),
        "log_count": len(job.get("logs", [])),
    }

@app.get("/dbt/logs/{job_id}")
async def dbt_logs(job_id: str, user: dict = Depends(require_auth)):
    """Get logs from a dbt job."""
    job = _dbt_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {
        "id": job["id"],
        "status": job["status"],
        "logs": job.get("logs", []),
        "tables": job.get("tables", []),
    }

@app.get("/dbt/jobs")
async def dbt_jobs(user: dict = Depends(require_auth)):
    """List dbt jobs for the current user."""
    email = user.get("email", "")
    jobs = [
        {"id": j["id"], "status": j["status"], "command": j["command"], 
         "duration": j.get("duration"), "tables": j.get("tables", []),
         "created_at": j.get("created_at")}
        for j in _dbt_jobs.values() if j.get("user") == email
    ]
    return {"jobs": sorted(jobs, key=lambda x: x.get("created_at", 0), reverse=True)}

# -- Native Iceberg REST API Proxy ---------------------------------------------
from fastapi import Request
from fastapi.responses import JSONResponse

@app.get("/v1/config")
def iceberg_config(warehouse: str = None):
    return {
        "defaults": {},
        "overrides": {"warehouse": f"s3://{tigris.BUCKET_NAME}/iceberg/"}
    }

@app.post("/v1/oauth/tokens")
def iceberg_oauth():
    return {"access_token": "duckpond-native-token", "token_type": "bearer", "expires_in": 864000}

@app.get("/v1/namespaces")
def list_namespaces():
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        namespaces = [row[0] for row in conn.execute("SELECT DISTINCT namespace FROM iceberg_tables").fetchall()]
    if "default" not in namespaces: namespaces.append("default")
    if "duckpond" not in namespaces: namespaces.append("duckpond")
    return {"namespaces": [[ns] for ns in namespaces]}

@app.post("/v1/namespaces")
async def create_namespace(request: Request):
    body = await request.json()
    ns = body.get("namespace", ["default"])
    return {"namespace": ns, "properties": {}}

@app.head("/v1/namespaces/{namespace}")
def head_namespace(namespace: str):
    return JSONResponse(content={}, status_code=204)

@app.get("/v1/namespaces/{namespace}")
def get_namespace(namespace: str):
    return {"namespace": [namespace], "properties": {}}

@app.get("/v1/namespaces/{namespace}/tables")
def list_tables(namespace: str):
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        rows = conn.execute("SELECT name FROM iceberg_tables WHERE namespace=?", (namespace,)).fetchall()
    return {"identifiers": [{"namespace": [namespace], "name": r[0]} for r in rows]}

@app.post("/v1/namespaces/{namespace}/tables")
async def create_table(namespace: str, request: Request):
    body = await request.json()
    t_name = body.get("name", "unknown")
    loc = body.get("location") or f"s3://{tigris.BUCKET_NAME}/iceberg/{namespace}/{t_name}"
    table_schema = body.get("schema", {"type": "struct", "fields": [], "schema-id": 0})
    
    table_uuid = str(uuid.uuid4())
    meta_loc = f"{loc}/metadata/00000-{table_uuid}.metadata.json"
    
    fields = table_schema.get("fields", [{"id": 0}])
    max_col_id = max([f.get("id", 0) for f in fields]) if fields else 0
    
    metadata = {
        "format-version": 2,
        "table-uuid": table_uuid,
        "location": loc,
        "last-sequence-number": 0,
        "last-updated-ms": int(time.time() * 1000),
        "last-column-id": max_col_id,
        "current-schema-id": table_schema.get("schema-id", 0),
        "schemas": [table_schema],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": body.get("properties", {}),
        "current-snapshot-id": -1,
        "refs": {},
        "snapshots": [],
        "statistics": [],
        "snapshot-log": [],
        "metadata-log": []
    }
    
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO iceberg_tables (namespace, name, metadata_location, schema_json) VALUES (?, ?, ?, ?)",
            (namespace, t_name, meta_loc, json.dumps(metadata))
        )
    
    return {
        "metadata-location": meta_loc,
        "metadata": metadata,
        "config": {}
    }

@app.post("/v1/namespaces/{namespace}/tables/{table}")
async def commit_table(namespace: str, table: str, request: Request):
    body = await request.json()
    log.info(f"Iceberg COMMIT for {namespace}.{table}")
    
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        row = conn.execute("SELECT metadata_location, schema_json FROM iceberg_tables WHERE namespace=? AND name=?", (namespace, table)).fetchone()

    if not row:
        return JSONResponse(status_code=404, content={"error": {"message": f"Table {table} not found", "type": "NoSuchTableException", "code": 404}})
        
    old_meta_loc, meta_str = row
    metadata = json.loads(meta_str)
    
    updates = body.get("updates", [])
    for u in updates:
        action = u.get("action")
        if action == "add-schema":
            metadata.setdefault("schemas", []).append(u.get("schema", {}))
        elif action == "set-current-schema":
            metadata["current-schema-id"] = u.get("schema-id", -1)
        elif action == "add-snapshot":
            metadata.setdefault("snapshots", []).append(u.get("snapshot", {}))
        elif action == "set-current-snapshot":
            metadata["current-snapshot-id"] = u.get("snapshot-id", -1)
        elif action == "add-partition-spec":
            metadata.setdefault("partition-specs", []).append(u.get("spec", {}))
        elif action == "set-default-partition-spec":
            metadata["default-spec-id"] = u.get("spec-id", -1)
        elif action == "add-sort-order":
            metadata.setdefault("sort-orders", []).append(u.get("sort-order", {}))
        elif action == "set-default-sort-order":
            metadata["default-sort-order-id"] = u.get("sort-order-id", -1)
        elif action == "upgrade-format-version":
            metadata["format-version"] = u.get("format-version", 2)
            
    metadata["last-updated-ms"] = int(time.time() * 1000)
    
    new_meta_loc = f"{metadata['location']}/metadata/00001-{uuid.uuid4()}.metadata.json"
    
    # Concurrent commit safety: use exclusive transaction to prevent race conditions
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        conn.execute("BEGIN EXCLUSIVE")
        # Re-read the current metadata to check for conflicts (optimistic concurrency)
        current_row = conn.execute("SELECT metadata_location FROM iceberg_tables WHERE namespace=? AND name=?", (namespace, table)).fetchone()
        if current_row and current_row[0] != old_meta_loc:
            conn.execute("ROLLBACK")
            return JSONResponse(status_code=409, content={"error": {"message": f"Commit conflict: table {table} was modified by another transaction. Please retry.", "type": "CommitFailedException", "code": 409}})
        conn.execute("UPDATE iceberg_tables SET metadata_location=?, schema_json=? WHERE namespace=? AND name=?", (new_meta_loc, json.dumps(metadata), namespace, table))
        conn.execute("COMMIT")

    return {
        "metadata-location": new_meta_loc,
        "metadata": metadata,
        "config": {}
    }

@app.get("/v1/namespaces/{namespace}/tables/{table}")
def load_table(namespace: str, table: str):
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        row = conn.execute("SELECT metadata_location, schema_json FROM iceberg_tables WHERE namespace=? AND name=?", (namespace, table)).fetchone()
    if not row:
        return JSONResponse(status_code=404, content={"error": {"message": f"Table {namespace}.{table} not found", "type": "NoSuchTableException", "code": 404}})
    
    meta_loc, meta_str = row
    meta_dict = json.loads(meta_str)
        
    return {
        "metadata-location": meta_loc,
        "metadata": meta_dict,
        "config": {}
    }

@app.delete("/v1/namespaces/{namespace}/tables/{table}")
def drop_table(namespace: str, table: str):
    log.info(f"Iceberg DROP for {namespace}.{table}")
    # Delete the S3 data files for this table
    try:
        s3 = tigris.get_s3_client()
        prefix = f"iceberg/{namespace}/{table}/"
        res = s3.list_objects_v2(Bucket=tigris.BUCKET_NAME, Prefix=prefix)
        if "Contents" in res:
            keys = [{"Key": obj["Key"]} for obj in res["Contents"]]
            s3.delete_objects(Bucket=tigris.BUCKET_NAME, Delete={"Objects": keys})
            log.info(f"Deleted {len(keys)} S3 objects under {prefix}")
    except Exception as e:
        log.warning(f"S3 cleanup failed for {namespace}.{table}: {e}")
    # Delete the catalog entry
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        conn.execute("DELETE FROM iceberg_tables WHERE namespace=? AND name=?", (namespace, table))
    return JSONResponse(status_code=204, content={})

@app.post("/admin/reset-catalog")
def reset_catalog():
    """Nuclear option: wipe all catalog entries and orphaned iceberg data."""
    log.warning("Admin: resetting entire Iceberg catalog")
    with sqlite3.connect(CATALOG_DB_PATH) as conn:
        conn.execute("DELETE FROM iceberg_tables")
    # Clean all iceberg data from S3
    try:
        s3 = tigris.get_s3_client()
        prefix = "iceberg/"
        res = s3.list_objects_v2(Bucket=tigris.BUCKET_NAME, Prefix=prefix)
        if "Contents" in res:
            keys = [{"Key": obj["Key"]} for obj in res["Contents"]]
            s3.delete_objects(Bucket=tigris.BUCKET_NAME, Delete={"Objects": keys})
            log.info(f"Purged {len(keys)} orphaned S3 objects under {prefix}")
    except Exception as e:
        log.warning(f"S3 purge failed: {e}")
    return {"status": "Catalog and S3 iceberg data wiped. Ready for fresh ingestion."}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), table_name: str = Form(...), user: dict = Depends(require_auth)):
    """Upload a local .parquet/.csv file to Tigris, then ingest into Iceberg."""
    tenant_ns = user.get("namespace", "duckpond")
    
    log.info(f"Upload received: {file.filename} ({file.size} bytes) → {tenant_ns}.{table_name}")
    
    # [PHASE 2] Storage Tracking Guard
    try:
        if user.get("id") != "system":
            db_res = auth_module.supabase.table("billing_profiles").select("storage_limit_bytes").eq("owner_id", user.get("id")).execute()
            if db_res.data:
                limit_bytes = db_res.data[0]["storage_limit_bytes"]
                s3_list = tigris.get_s3_client().list_objects_v2(Bucket=tigris.BUCKET_NAME, Prefix=f"iceberg/{tenant_ns}/")
                current_size = sum(obj.get("Size", 0) for obj in s3_list.get("Contents", []))
                if current_size + file.size > limit_bytes:
                    raise HTTPException(status_code=402, detail=f"Storage Quota Exceeded. Used: {current_size/1024**3:.2f}GB / Limit: {limit_bytes/1024**3:.2f}GB")
    except HTTPException: raise
    except Exception as e: log.error(f"Quota check error: {e}")
    
    # Sanitize table name
    import re
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
    
    # Stage file to Tigris
    ext = os.path.splitext(file.filename or 'data.parquet')[1].lower()
    staging_key = f"uploads/{tenant_ns}/{table_name}/{uuid.uuid4().hex[:8]}{ext}"
    
    try:
        s3 = tigris.get_s3_client()
        contents = await file.read()
        s3.put_object(Bucket=tigris.BUCKET_NAME, Key=staging_key, Body=contents)
        log.info(f"Staged {len(contents)} bytes to s3://{tigris.BUCKET_NAME}/{staging_key}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")
    
    # Inspect schema to promote Iceberg-incompatible types and detect sort column
    select_expr = "*"
    order_by_clause = ""
    if ext == '.parquet':
        try:
            import pyarrow.parquet as pq
            schema = pq.read_schema(io.BytesIO(contents))
            # Map pyarrow types to Iceberg-compatible DuckDB casts
            promotions = []
            safe_cols = []
            sort_col = None
            for field in schema:
                pa_type = field.type
                if pa_type in (pa.int8(), pa.uint8(), pa.int16(), pa.uint16()):
                    promotions.append(f'"{field.name}"::INTEGER AS "{field.name}"')
                elif pa_type in (pa.uint32(),):
                    promotions.append(f'"{field.name}"::BIGINT AS "{field.name}"')
                elif pa_type in (pa.uint64(),):
                    promotions.append(f'"{field.name}"::BIGINT AS "{field.name}"')
                else:
                    safe_cols.append(f'"{field.name}"')
                # Auto-detect best sort column: first date/timestamp column
                if sort_col is None and (pa.types.is_date(pa_type) or pa.types.is_timestamp(pa_type)):
                    sort_col = field.name

            if promotions:
                select_expr = ", ".join(safe_cols + promotions)
                log.info(f"Auto-promoting {len(promotions)} columns for Iceberg compatibility")
            if sort_col:
                order_by_clause = f' ORDER BY "{sort_col}"'
                log.info(f"Auto-sort on '{sort_col}' for optimal row-group pruning")
        except Exception as e:
            log.warning(f"Schema inspection failed, using SELECT *: {e}")
    
    # Now ingest from the staged S3 path into Iceberg
    tig_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    tig_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    tig_ep = tigris.TIGRIS_ENDPOINT.replace("https://", "")
    
    iceberg_binds = f"""
    CREATE SECRET IF NOT EXISTS tigris_s3 (TYPE S3, KEY_ID '{tig_key}', SECRET '{tig_secret}', ENDPOINT '{tig_ep}');
    CREATE SECRET IF NOT EXISTS iceberg_auth (TYPE ICEBERG, TOKEN '{os.environ.get("INTERNAL_AUTH_TOKEN", "duckpond-internal")}');
    ATTACH IF NOT EXISTS 'duckpond' AS enterprise_lake (
        TYPE ICEBERG,
        ENDPOINT 'https://duckpond-coordinator.fly.dev'
    );
    """
    
    staged_uri = f"s3://{tigris.BUCKET_NAME}/{staging_key}"
    read_fn = "read_csv_auto" if ext == '.csv' else "read_parquet"
    sql = f"{iceberg_binds}\nDROP TABLE IF EXISTS enterprise_lake.{tenant_ns}.{table_name};\nCREATE TABLE enterprise_lake.{tenant_ns}.{table_name} AS SELECT {select_expr} FROM {read_fn}('{staged_uri}'){order_by_clause};"
    
    try:
        resp = stream_via_fly_workers(sql, [], limit=100, output_mode="grid", compute_tier="enterprise")
        async for chunk_bytes in resp.body_iterator:
            chunk_str = chunk_bytes.decode('utf-8')
            log.info(f"Upload Worker: {chunk_str}")
            try:
                msg = json.loads(chunk_str)
                if msg.get("type") == "error":
                    return {"error": f"Ingestion failed: {msg.get('msg', 'Unknown')}"}
            except: pass
        # Clean up staging file after successful ingestion
        try:
            s3 = tigris.get_s3_client()
            s3.delete_object(Bucket=tigris.BUCKET_NAME, Key=staging_key)
            log.info(f"Cleaned up staging file: {staging_key}")
        except Exception as e:
            log.warning(f"Staging cleanup failed (non-critical): {e}")
        
        return {"status": f"{file.filename} uploaded and ingested as '{table_name}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion error: {str(e)}")

# ------------------------------------------------------------------------------
@app.post("/stage")
async def stage_file(file: UploadFile = File(...), table_name: str = Form(...), user: dict = Depends(require_auth)):
    """Stage a parquet file to Tigris without ingesting. Returns the S3 URI."""
    tenant_ns = user.get("namespace", "duckpond")

    import re
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
    ext = os.path.splitext(file.filename or 'data.parquet')[1].lower()
    staging_key = f"uploads/{tenant_ns}/{table_name}/{uuid.uuid4().hex[:8]}{ext}"

    try:
        s3 = tigris.get_s3_client()
        contents = await file.read()
        s3.put_object(Bucket=tigris.BUCKET_NAME, Key=staging_key, Body=contents)
        staged_uri = f"s3://{tigris.BUCKET_NAME}/{staging_key}"
        log.info(f"Staged {len(contents)} bytes to {staged_uri}")
        return {"status": "staged", "uri": staged_uri, "namespace": tenant_ns}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stage: {str(e)}")

# ------------------------------------------------------------------------------
@app.post("/upload-from-staged")
async def upload_from_staged(request: Request, user: dict = Depends(require_auth)):
    """Create an Iceberg table from multiple staged S3 files in one operation."""
    body = await request.json()
    table_name = body.get("table_name")
    staged_uris = body.get("uris", [])

    if not table_name or not staged_uris:
        raise HTTPException(status_code=400, detail="table_name and uris are required")

    tenant_ns = user.get("namespace", "duckpond")

    import re
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

    tig_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    tig_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    tig_ep = tigris.TIGRIS_ENDPOINT.replace("https://", "")

    iceberg_binds = f"""
    CREATE SECRET IF NOT EXISTS tigris_s3 (TYPE S3, KEY_ID '{tig_key}', SECRET '{tig_secret}', ENDPOINT '{tig_ep}');
    CREATE SECRET IF NOT EXISTS iceberg_auth (TYPE ICEBERG, TOKEN '{os.environ.get("INTERNAL_AUTH_TOKEN", "duckpond-internal")}');
    ATTACH IF NOT EXISTS 'duckpond' AS enterprise_lake (
        TYPE ICEBERG,
        ENDPOINT 'https://duckpond-coordinator.fly.dev'
    );
    """

    # Build read_parquet with list of all staged URIs
    # Note: no ORDER BY for multi-file uploads — sorting 60M+ rows would OOM the worker.
    # Sorting is applied only in /upload (single-file, schema-inspected) path.
    uri_list = ", ".join(f"'{u}'" for u in staged_uris)
    sql = f"{iceberg_binds}\nDROP TABLE IF EXISTS enterprise_lake.{tenant_ns}.{table_name};\nCREATE TABLE enterprise_lake.{tenant_ns}.{table_name} AS SELECT * FROM read_parquet([{uri_list}]);"

    log.info(f"Upload-from-staged: {table_name} from {len(staged_uris)} files in {tenant_ns}")

    try:
        resp = stream_via_fly_workers(sql, [], limit=100, output_mode="grid", compute_tier="enterprise")
        async for chunk_bytes in resp.body_iterator:
            chunk_str = chunk_bytes.decode('utf-8')
            log.info(f"Upload-staged Worker: {chunk_str}")
            try:
                msg = json.loads(chunk_str)
                if msg.get("type") == "error":
                    return {"error": f"Ingestion failed: {msg.get('msg', 'Unknown')}"}
            except: pass

        # Clean up staging files
        try:
            s3 = tigris.get_s3_client()
            for uri in staged_uris:
                key = uri.replace(f"s3://{tigris.BUCKET_NAME}/", "")
                s3.delete_object(Bucket=tigris.BUCKET_NAME, Key=key)
            log.info(f"Cleaned up {len(staged_uris)} staging files")
        except Exception as e:
            log.warning(f"Staging cleanup failed: {e}")

        return {"status": f"Created table '{table_name}' from {len(staged_uris)} files"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion error: {str(e)}")

# ------------------------------------------------------------------------------
@app.post("/upload-append")
async def upload_append(file: UploadFile = File(...), table_name: str = Form(...), user: dict = Depends(require_auth)):
    """Append data to an existing Iceberg table (used for chunked uploads)."""
    tenant_ns = user.get("namespace", "duckpond")

    log.info(f"Upload-append received: {file.filename} ({file.size} bytes) → {tenant_ns}.{table_name}")

    import re
    table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

    ext = os.path.splitext(file.filename or 'data.parquet')[1].lower()
    staging_key = f"uploads/{tenant_ns}/{table_name}/{uuid.uuid4().hex[:8]}{ext}"

    try:
        s3 = tigris.get_s3_client()
        contents = await file.read()
        s3.put_object(Bucket=tigris.BUCKET_NAME, Key=staging_key, Body=contents)
        log.info(f"Staged {len(contents)} bytes to s3://{tigris.BUCKET_NAME}/{staging_key}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")

    tig_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    tig_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    tig_ep = tigris.TIGRIS_ENDPOINT.replace("https://", "")

    iceberg_binds = f"""
    CREATE SECRET IF NOT EXISTS tigris_s3 (TYPE S3, KEY_ID '{tig_key}', SECRET '{tig_secret}', ENDPOINT '{tig_ep}');
    CREATE SECRET IF NOT EXISTS iceberg_auth (TYPE ICEBERG, TOKEN '{os.environ.get("INTERNAL_AUTH_TOKEN", "duckpond-internal")}');
    ATTACH IF NOT EXISTS 'duckpond' AS enterprise_lake (
        TYPE ICEBERG,
        ENDPOINT 'https://duckpond-coordinator.fly.dev'
    );
    """

    staged_uri = f"s3://{tigris.BUCKET_NAME}/{staging_key}"
    read_fn = "read_csv_auto" if ext == '.csv' else "read_parquet"
    sql = f"{iceberg_binds}\nINSERT INTO enterprise_lake.{tenant_ns}.{table_name} SELECT * FROM {read_fn}('{staged_uri}');"

    try:
        resp = stream_via_fly_workers(sql, [], limit=100, output_mode="grid", compute_tier="enterprise")
        async for chunk_bytes in resp.body_iterator:
            chunk_str = chunk_bytes.decode('utf-8')
            log.info(f"Upload-append Worker: {chunk_str}")
            try:
                msg = json.loads(chunk_str)
                if msg.get("type") == "error":
                    return {"error": f"Append failed: {msg.get('msg', 'Unknown')}"}
            except: pass
        try:
            s3 = tigris.get_s3_client()
            s3.delete_object(Bucket=tigris.BUCKET_NAME, Key=staging_key)
        except Exception as e:
            log.warning(f"Staging cleanup failed: {e}")

        return {"status": f"Appended {file.filename} to '{table_name}'"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Append error: {str(e)}")

# ------------------------------------------------------------------------------

def _cleanup_file(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
            log.info(f"Cleaned up temp export: {path}")
    except Exception as e:
        log.error(f"Cleanup failed: {e}")

@app.post("/export/parquet")
async def export_parquet(req: QueryRequest, background_tasks: BackgroundTasks, user: dict = Depends(require_auth)):
    """Backend Parquet export: executes SQL and returns a physical Parquet file."""
    tenant_ns = user.get("namespace", "duckpond")
    
    # Simple rate limiting check
    if not _rate_limiter.acquire(tenant_ns):
        raise HTTPException(status_code=429, detail="Too many concurrent queries")

    tmp_id = str(uuid.uuid4())
    tmp_path = f"/tmp/export_{tmp_id}.parquet"
    
    try:
        # Use DuckDB directly for binary generation
        # We wrap the user SQL in the COPY command
        export_sql = f"COPY ({req.sql}) TO '{tmp_path}' (FORMAT PARQUET)"
        
        # Execute query
        # Reuse existing _run_sql_logic or similar if available, 
        # but for simplicity we can use duckdb directly since we are on the coordinator.
        conn = duckdb.connect()
        # Setup search path if needed (though _execute_query usually handles it)
        # But for exports, we just run it.
        # Note: If they use Iceberg tables, we need the worker logic.
        # For simplicity, we assume Standard SQL (which is what Data Loader/Landing uses).
        conn.execute(export_sql)
        conn.close()

        background_tasks.add_task(_cleanup_file, tmp_path)
        
        return FileResponse(
            path=tmp_path,
            filename=f"shikipond_export_{int(time.time())}.parquet",
            media_type="application/octet-stream"
        )
        
    except Exception as e:
        if os.path.exists(tmp_path): os.remove(tmp_path)
        log.error(f"Parquet export failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        _rate_limiter.release(tenant_ns)

@app.post("/query")
def query(req: QueryRequest, user: dict = Depends(require_auth)):
    t0 = time.time()
    tenant_ns = user.get("namespace", "duckpond")

    # [PHASE 3] Compute Pre-Flight Quota Ledger Check
    if not LOCAL_MODE and req.compute_tier != "micro" and user.get("id") != "system":
        try:
            db_res = auth_module.supabase.table("billing_profiles").select("id, compute_credit_balance, compute_rate").eq("owner_id", user.get("id")).execute()
            if db_res.data:
                profile = db_res.data[0]
                bal = float(profile["compute_credit_balance"])
                rate = float(profile["compute_rate"])
                # Tier multiplier (Distributed Heavy = 1.0x, Standard = 0.25x)
                cost = rate if req.compute_tier == "distributed" else rate * 0.25
                
                if bal < cost:
                    return {"error": f"Insufficient credits. Balance: ${bal:.3f}. Required: ${cost:.3f}. Please upgrade your account.", "data": []}
                
                # Pre-deduct
                new_bal = bal - cost
                auth_module.supabase.table("billing_profiles").update({"compute_credit_balance": new_bal}).eq("id", profile["id"]).execute()
                log.info(f"Deducted ${cost:.3f} from profile {profile['id']}. New balance: ${new_bal:.3f}")
        except Exception as e:
            log.error(f"Compute check failed: {e}")
    
    # Extract user email for audit
    user_email = user.get("email", "")
    
    # Rate Limiting: reject if tenant has too many concurrent queries
    if not _rate_limiter.acquire(tenant_ns):
        return {"error": f"Rate limit exceeded: you have {MAX_CONCURRENT_QUERIES_PER_TENANT} concurrent queries running. Please wait for existing queries to complete.", "data": []}
    
    # Log query start
    query_id = None
    try:
        with sqlite3.connect(CATALOG_DB_PATH) as conn:
            cursor = conn.execute(
                "INSERT INTO query_history (namespace, user_email, sql_text) VALUES (?, ?, ?)",
                (tenant_ns, user_email, req.sql[:2000])  # Truncate long queries
            )
            query_id = cursor.lastrowid
    except Exception:
        pass  # Don't block queries if audit logging fails
    
    try:
        result = _execute_query(req, tenant_ns, t0)
        
        # Log query completion
        if query_id:
            duration_ms = round((time.time() - t0) * 1000)
            status = "ok"
            error_msg = None
            if isinstance(result, dict):
                if result.get("error"):
                    status = "error"
                    error_msg = str(result["error"])[:500]
            try:
                with sqlite3.connect(CATALOG_DB_PATH) as conn:
                    conn.execute(
                        "UPDATE query_history SET status=?, error_msg=?, duration_ms=? WHERE id=?",
                        (status, error_msg, duration_ms, query_id)
                    )
            except Exception:
                pass
        
        return result
    except Exception as e:
        if query_id:
            duration_ms = round((time.time() - t0) * 1000)
            try:
                with sqlite3.connect(CATALOG_DB_PATH) as conn:
                    conn.execute(
                        "UPDATE query_history SET status='error', error_msg=?, duration_ms=? WHERE id=?",
                        (str(e)[:500], duration_ms, query_id)
                    )
            except Exception:
                pass
        raise
    finally:
        _rate_limiter.release(tenant_ns)

def _execute_query(req: QueryRequest, tenant_ns: str, t0: float):
    """Inner query execution, wrapped by rate limiter and audit logging."""
    import re
    # 1. Handle explicit legacy table aliases like {files_movies} or {files_actors}
    aliases = re.findall(r'\{files_([a-zA-Z0-9_]+)\}', req.sql)
    for table in aliases:
        files, ext = tigris.list_table_files(f"data/{table}/", presign=True)
        if not files:
            return {"status": "ok", "error": f"No data found for table {table}"}
        files_str = ", ".join([f"'{f}'" for f in files])
        req.sql = req.sql.replace(f"{{files_{table}}}", f"[{files_str}]")
        
    # 2. Handle the generic default UI query {files} 
    if "{files}" in req.sql:
        files, ext = tigris.list_table_files(f"data/{req.table_name}/", presign=True)
        if not files:
            return {"status": "ok", "error": "No data found for this table"}
        files_str = ", ".join([f"'{f}'" for f in files])
        req.sql = req.sql.replace("{files}", f"[{files_str}]")
        
    # 3. V3 AST S3 Native Routing Architecture
    try:
        import sqlglot
        import sqlglot.expressions as exp
        
        parsed = sqlglot.parse_one(req.sql, read="duckdb")
        log.info(f"AST Rewrite: tenant_ns={tenant_ns}, sql={req.sql[:100]}")
        
        # 3a. Capture pure DML targets before AST mutation destroys them
        if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete)):
            _target_node = parsed.this
            _raw_name = _target_node.name if _target_node else ""
            _raw_db = None
            if hasattr(_target_node, 'db') and _target_node.db:
                _raw_db = _target_node.db
            elif hasattr(_target_node, 'table') and _target_node.table:
                _raw_db = _target_node.table
            
            parsed._dml_target_table = _raw_name
            if not _raw_db or _raw_db == "duckpond":
                parsed._dml_target_ns = tenant_ns
            else:
                parsed._dml_target_ns = _raw_db
        
        # 3b. Intercept Iceberg View Definitions
        if isinstance(parsed, exp.Create) and str(parsed.args.get("kind", "")).upper() == "VIEW":
            view_name = parsed.this.name
            view_sql = parsed.expression.sql(dialect="duckdb")
            
            import uuid
            view_json = {
                "view-uuid": str(uuid.uuid4()),
                "format-version": 1,
                "location": f"s3://duckpond-data/views/{tenant_ns}/{view_name}",
                "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
                "current-schema-id": 0,
                "versions": [
                    {
                        "version-id": 1,
                        "timestamp-ms": int(time.time() * 1000),
                        "summary": {"operation": "create"},
                        "representations": [
                            {
                                "type": "sql",
                                "sql": view_sql,
                                "dialect": "duckdb"
                            }
                        ]
                    }
                ],
                "current-version-id": 1
            }
            
            with sqlite3.connect(CATALOG_DB_PATH) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO iceberg_tables (namespace, name, type, schema_json) VALUES (?, ?, 'view', ?)",
                    (tenant_ns, view_name, json.dumps(view_json))
                )
            return {"status": "ok", "msg": f"Success! Native Iceberg View '{view_name}' registered.", "data": []}
        
        # 3b2. Intercept Iceberg CREATE TABLE Definitions
        if isinstance(parsed, exp.Create) and str(parsed.args.get("kind", "")).upper() == "TABLE":
            schema_node = parsed.this  # This is a Schema node wrapping the Table
            tbl_node = schema_node.this if hasattr(schema_node, 'this') else schema_node
            tbl_name = tbl_node.name
            
            # Map DuckDB/SQL types to Iceberg types
            type_map = {
                "INT": "int", "INTEGER": "int", "BIGINT": "long", "SMALLINT": "int",
                "TINYINT": "int", "FLOAT": "float", "DOUBLE": "double", "REAL": "float",
                "BOOLEAN": "boolean", "BOOL": "boolean",
                "VARCHAR": "string", "TEXT": "string", "STRING": "string", "CHAR": "string",
                "DATE": "date", "TIMESTAMP": "timestamptz", "TIMESTAMP WITH TIME ZONE": "timestamptz",
                "DECIMAL": "decimal", "NUMERIC": "decimal",
                "BLOB": "binary", "BYTEA": "binary",
            }
            
            iceberg_fields = []
            for i, col_def in enumerate(schema_node.expressions):
                col_name = col_def.name
                col_type_sql = col_def.args.get("kind", "").sql(dialect="duckdb").upper() if col_def.args.get("kind") else "STRING"
                iceberg_type = type_map.get(col_type_sql, "string")
                iceberg_fields.append({
                    "id": i + 1,
                    "name": col_name,
                    "required": False,
                    "type": iceberg_type
                })
            
            import uuid
            table_uuid = str(uuid.uuid4())
            loc = f"s3://{tigris.BUCKET_NAME}/iceberg/{tenant_ns}/{tbl_name}"
            meta_loc = f"{loc}/metadata/00000-{table_uuid}.metadata.json"
            
            metadata = {
                "format-version": 2,
                "table-uuid": table_uuid,
                "location": loc,
                "last-sequence-number": 0,
                "last-updated-ms": int(time.time() * 1000),
                "last-column-id": len(iceberg_fields),
                "current-schema-id": 0,
                "schemas": [{"type": "struct", "schema-id": 0, "fields": iceberg_fields}],
                "default-spec-id": 0,
                "partition-specs": [{"spec-id": 0, "fields": []}],
                "last-partition-id": 999,
                "default-sort-order-id": 0,
                "sort-orders": [{"order-id": 0, "fields": []}],
                "properties": {},
                "current-snapshot-id": -1,
                "refs": {},
                "snapshots": [],
                "statistics": [],
                "snapshot-log": [],
                "metadata-log": []
            }
            
            with sqlite3.connect(CATALOG_DB_PATH) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO iceberg_tables (namespace, name, metadata_location, schema_json) VALUES (?, ?, ?, ?)",
                    (tenant_ns, tbl_name, meta_loc, json.dumps(metadata))
                )
            
            col_summary = ", ".join([f"{f['name']} ({f['type']})" for f in iceberg_fields])
            return {"status": "ok", "msg": f"Success! Iceberg Table '{tbl_name}' created with columns: {col_summary}", "data": []}
        
        # 3b3. Intercept DROP TABLE
        if isinstance(parsed, exp.Drop) and str(parsed.args.get("kind", "")).upper() == "TABLE":
            drop_tbl_name = parsed.this.name
            drop_db = getattr(parsed.this, 'db', None)
            
            if not drop_db or drop_db == "duckpond":
                drop_ns = tenant_ns
            else:
                drop_ns = drop_db
            
            # Check if table exists in catalog
            with sqlite3.connect(CATALOG_DB_PATH) as conn:
                row = conn.execute("SELECT name FROM iceberg_tables WHERE namespace=? AND name=?", (drop_ns, drop_tbl_name)).fetchone()
            
            if not row:
                if parsed.args.get("exists"):  # DROP TABLE IF EXISTS
                    return {"status": "ok", "msg": f"Table '{drop_tbl_name}' does not exist (IF EXISTS — no error).", "data": []}
                return {"error": f"Table '{drop_tbl_name}' not found in namespace '{drop_ns}'."}
            
            # Clean up S3 data files
            try:
                s3 = tigris.get_s3_client()
                prefix = f"iceberg/{drop_ns}/{drop_tbl_name}/"
                res = s3.list_objects_v2(Bucket=tigris.BUCKET_NAME, Prefix=prefix)
                if "Contents" in res:
                    keys = [{"Key": obj["Key"]} for obj in res["Contents"]]
                    s3.delete_objects(Bucket=tigris.BUCKET_NAME, Delete={"Objects": keys})
                    log.info(f"DROP TABLE: cleaned {len(keys)} S3 objects under {prefix}")
            except Exception as e:
                log.warning(f"DROP TABLE S3 cleanup failed for {drop_ns}.{drop_tbl_name}: {e}")
            
            # Remove from catalog
            with sqlite3.connect(CATALOG_DB_PATH) as conn:
                conn.execute("DELETE FROM iceberg_tables WHERE namespace=? AND name=?", (drop_ns, drop_tbl_name))
            
            return {"status": "ok", "msg": f"Success! Table '{drop_tbl_name}' dropped.", "data": []}
        
        # 3b4. Intercept ALTER TABLE (schema evolution)
        if isinstance(parsed, exp.Alter):
            alter_tbl_name = parsed.this.name
            alter_db = getattr(parsed.this, 'db', None)
            
            if not alter_db or alter_db == "duckpond":
                alter_ns = tenant_ns
            else:
                alter_ns = alter_db
            
            with sqlite3.connect(CATALOG_DB_PATH) as conn:
                row = conn.execute("SELECT schema_json FROM iceberg_tables WHERE namespace=? AND name=?", (alter_ns, alter_tbl_name)).fetchone()
            
            if not row:
                return {"error": f"Table '{alter_tbl_name}' not found in namespace '{alter_ns}'."}
            
            metadata = json.loads(row[0])
            current_schema = metadata.get("schemas", [{}])[-1]
            fields = current_schema.get("fields", [])
            
            type_map = {
                "INT": "int", "INTEGER": "int", "BIGINT": "long", "SMALLINT": "int",
                "TINYINT": "int", "FLOAT": "float", "DOUBLE": "double", "REAL": "float",
                "BOOLEAN": "boolean", "BOOL": "boolean",
                "VARCHAR": "string", "TEXT": "string", "STRING": "string", "CHAR": "string",
                "DATE": "date", "TIMESTAMP": "timestamptz",
                "DECIMAL": "decimal", "NUMERIC": "decimal",
                "BLOB": "binary", "BYTEA": "binary",
            }
            
            changes = []
            for action in parsed.actions:
                if isinstance(action, exp.ColumnDef):
                    # ADD COLUMN
                    col_name = action.name
                    col_type_raw = str(action.args.get("kind", "")) if action.args.get("kind") else "STRING"
                    col_type = type_map.get(col_type_raw.upper(), "string")
                    max_id = max([f.get("id", 0) for f in fields], default=0)
                    fields.append({"id": max_id + 1, "name": col_name, "required": False, "type": col_type})
                    changes.append(f"+ {col_name} ({col_type})")
                elif isinstance(action, exp.Drop):
                    # DROP COLUMN
                    col_name = action.this.name if hasattr(action.this, 'name') else str(action.this)
                    fields = [f for f in fields if f["name"] != col_name]
                    changes.append(f"- {col_name}")
            
            current_schema["fields"] = fields
            metadata["last-column-id"] = max([f.get("id", 0) for f in fields], default=0)
            metadata["last-updated-ms"] = int(time.time() * 1000)
            
            with sqlite3.connect(CATALOG_DB_PATH) as conn:
                conn.execute("UPDATE iceberg_tables SET schema_json=? WHERE namespace=? AND name=?", (json.dumps(metadata), alter_ns, alter_tbl_name))
            
            changes_str = ", ".join(changes)
            return {"status": "ok", "msg": f"Success! Table '{alter_tbl_name}' altered: {changes_str}", "data": []}
            
        # 3c. Dynamic S3 Parquet format mapper and View Expansion
        for table in parsed.find_all(exp.Table):
            table_name = table.name
            if not table_name: continue
            
            # DO NOT rewrite destination tables in CREATE, INSERT, UPDATE, or DELETE statements
            if isinstance(parsed, (exp.Create, exp.Insert, exp.Update, exp.Delete)):
                dest = parsed.this
                if dest is table or (hasattr(dest, 'this') and dest.this is table):
                    continue
            
            # Priority 1: Check Iceberg catalog first (views and managed tables)
            with sqlite3.connect(CATALOG_DB_PATH) as conn:
                row = conn.execute("SELECT type, schema_json FROM iceberg_tables WHERE namespace=? AND name=?", (tenant_ns, table_name)).fetchone()
            if row:
                tbl_type, sch_json = row
                if tbl_type == 'view':
                    try:
                        meta = json.loads(sch_json)
                        view_sql = meta.get("versions", [{}])[-1].get("representations", [{}])[0].get("sql")
                        if view_sql:
                            new_node = sqlglot.parse_one(f"({view_sql})", read="duckdb")
                            if table.alias:
                                new_node = exp.alias_(new_node, table.alias)
                            else:
                                new_node = exp.alias_(new_node, table_name)
                            table.replace(new_node)
                            continue
                    except Exception as e:
                        log.error(f"Failed expanding view {table_name}: {e}")
                    
                # Standard Iceberg Table — resolve to direct read_parquet() on data files (no ATTACH needed)
                try:
                    meta = json.loads(sch_json)
                    tbl_location = meta.get("location", f"s3://{tigris.BUCKET_NAME}/iceberg/{tenant_ns}/{table_name}")
                    data_glob = f"{tbl_location}/data/*.parquet"
                    scan_expr = f"read_parquet('{data_glob}', hive_partitioning = false)"
                    new_node = sqlglot.parse_one(scan_expr, read="duckdb")
                    if table.alias:
                        new_node = exp.alias_(new_node, table.alias)
                    else:
                        new_node = exp.alias_(new_node, table_name)
                    table.replace(new_node)
                except Exception as e:
                    log.warning(f"read_parquet rewrite failed for {table_name}, falling back to enterprise_lake: {e}")
                    new_node = sqlglot.parse_one(f"enterprise_lake.{tenant_ns}.{table_name}", read="duckdb")
                    if table.alias:
                        new_node = exp.alias_(new_node, table.alias)
                    table.replace(new_node)
                continue
                
            # Priority 2: Fallback to legacy Tigris raw dataset files
            files, ext = tigris.list_table_files(f"data/{table_name}/", presign=True)
            if files:
                files_str = ", ".join([f"'{f}'" for f in files])
                
                # Dynamic format mapper 
                if ext == '.csv':
                    inject_cmd = f"read_csv_auto([{files_str}])"
                elif ext == '.json':
                    inject_cmd = f"read_json_auto([{files_str}])"
                else:
                    inject_cmd = f"read_parquet([{files_str}])"
                    
                new_node = sqlglot.parse_one(inject_cmd, read="duckdb")
                if table.alias:
                    new_node = exp.alias_(new_node, table.alias)
                table.replace(new_node)
                
        req.sql = parsed.sql(dialect="duckdb")
        
        # 3c. CROSS-TENANT ISOLATION GUARD
        # After all AST rewrites, scan the final SQL for any enterprise_lake.{ns} references.
        # If a user manually typed enterprise_lake.org_other_tenant.secret_table, block it.
        import re as _re
        cross_tenant_refs = _re.findall(r'enterprise_lake\.(\w+)\.', req.sql, _re.IGNORECASE)
        for ref_ns in cross_tenant_refs:
            if ref_ns != tenant_ns:
                log.warning(f"SECURITY: Cross-tenant access blocked! User namespace={tenant_ns} tried to access namespace={ref_ns}")
                return {"error": f"Access denied: you do not have permission to access namespace '{ref_ns}'.", "data": []}
        
        # 3d. PyIceberg Transactional DML Commits
        if isinstance(parsed, exp.Insert):
            # Because sqlglot rewrites mutate parsed.this unexpectedly (turning into a Column or Schema losing its name),
            # we MUST capture it cleanly while it is preserved.
            if not getattr(parsed, '_dml_target_table', None):
                target_table_name = parsed.this.name
                target_ns = tenant_ns
                if hasattr(parsed.this, 'table') and parsed.this.table:
                    target_ns = parsed.this.table
                elif hasattr(parsed.this, 'db') and parsed.this.db:
                    target_ns = parsed.this.db
            else:
                target_table_name = parsed._dml_target_table
                target_ns = parsed._dml_target_ns
            
            try:
                from pyiceberg.catalog.rest import RestCatalog
                import duckdb
                import pyarrow as pa
                
                aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin" if os.environ.get("LOCAL_MODE") else "")
                aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin" if os.environ.get("LOCAL_MODE") else "")
                s3_endpoint = f"http://localhost:9000" if os.environ.get("LOCAL_MODE") else getattr(tigris, "TIGRIS_ENDPOINT", "https://fly.storage.tigris.dev")
                s3_endpoint = os.environ.get("AWS_ENDPOINT_URL_S3", s3_endpoint)
                
                catalog = RestCatalog(
                    "duckpond",
                    uri="http://localhost:8081",
                    token=os.environ.get("INTERNAL_AUTH_TOKEN", ""),
                    **{
                        "s3.endpoint": s3_endpoint,
                        "s3.access-key-id": aws_access_key_id,
                        "s3.secret-access-key": aws_secret_access_key,
                        "s3.region": os.environ.get("AWS_REGION", "auto")
                    }
                )
                
                table = catalog.load_table((target_ns, target_table_name))
                
                local_conn = duckdb.connect()
                local_conn.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")
                
                # DuckDB doesn't natively map local REST queries unless using HTTPFS
                endpoint_strip = s3_endpoint.replace('https://', '').replace('http://', '')
                local_conn.execute(f"SET s3_endpoint='{endpoint_strip}'")
                local_conn.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
                local_conn.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
                local_conn.execute("SET s3_use_ssl=" + ("false" if os.environ.get("LOCAL_MODE") else "true"))
                local_conn.execute("SET s3_url_style='path'" if os.environ.get("LOCAL_MODE") else "SET s3_url_style='vhost'")
                
                subquery_sql = parsed.expression.sql(dialect="duckdb")
                log.info(f"DML Local DuckDB evaluation: {subquery_sql}")

                # Rename helper: align Arrow column names to Iceberg schema
                iceberg_schema = table.schema()
                expected_names = [field.name for field in iceberg_schema.fields]

                def _rename_if_needed(arrow_tbl):
                    if len(expected_names) == len(arrow_tbl.column_names):
                        return arrow_tbl.rename_columns(expected_names)
                    return arrow_tbl

                # Stream in batches to avoid OOM on large INSERTs
                BATCH_SIZE = 500_000  # rows per batch
                total_rows = 0
                reader = local_conn.execute(subquery_sql).fetch_record_batch(BATCH_SIZE)

                while True:
                    try:
                        batch = reader.read_next_batch()
                    except StopIteration:
                        break

                    if batch.num_rows == 0:
                        continue

                    arrow_chunk = pa.Table.from_batches([batch])
                    arrow_chunk = _rename_if_needed(arrow_chunk)

                    if parsed.args.get('overwrite') and total_rows == 0:
                        table.overwrite(arrow_chunk)
                    else:
                        table.append(arrow_chunk)

                    total_rows += batch.num_rows
                    log.info(f"DML batch: {total_rows:,} rows committed so far")

                local_conn.close()
                return {"status": "ok", "msg": f"Success! Transactional DML mutated {total_rows:,} rows in Iceberg.", "data": []}
            except Exception as e:
                log.error(f"DML Transaction Failed: {e}")
                return {"error": f"DML Failed: {str(e)}"}
        
        # 3e. UPDATE / DELETE via Copy-on-Write
        if isinstance(parsed, (exp.Update, exp.Delete)):
            target_table_name = getattr(parsed, '_dml_target_table', '') or parsed.this.name
            target_ns = getattr(parsed, '_dml_target_ns', tenant_ns)
            
            try:
                from pyiceberg.catalog.rest import RestCatalog
                import duckdb as _duckdb
                import pyarrow as pa
                
                aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin" if os.environ.get("LOCAL_MODE") else "")
                aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin" if os.environ.get("LOCAL_MODE") else "")
                s3_endpoint = f"http://localhost:9000" if os.environ.get("LOCAL_MODE") else getattr(tigris, "TIGRIS_ENDPOINT", "https://fly.storage.tigris.dev")
                s3_endpoint = os.environ.get("AWS_ENDPOINT_URL_S3", s3_endpoint)
                
                catalog = RestCatalog(
                    "duckpond",
                    uri="http://localhost:8081",
                    token=os.environ.get("INTERNAL_AUTH_TOKEN", ""),
                    **{
                        "s3.endpoint": s3_endpoint,
                        "s3.access-key-id": aws_access_key_id,
                        "s3.secret-access-key": aws_secret_access_key,
                        "s3.region": os.environ.get("AWS_REGION", "auto")
                    }
                )
                
                iceberg_table = catalog.load_table((target_ns, target_table_name))
                
                # Read existing data from Iceberg into Arrow
                scan = iceberg_table.scan()
                existing_df = scan.to_arrow()
                
                if len(existing_df) == 0 and isinstance(parsed, exp.Delete):
                    return {"status": "ok", "msg": f"Table '{target_table_name}' is already empty. 0 rows affected.", "data": []}
                
                # Load into a local DuckDB and apply the mutation
                local_conn = _duckdb.connect()
                local_conn.execute("CREATE TABLE _target AS SELECT * FROM existing_df")
                
                if isinstance(parsed, exp.Update):
                    # Build UPDATE on the local table
                    set_clauses = ", ".join([e.sql(dialect="duckdb") for e in parsed.args.get("expressions", [])])
                    where_clause = parsed.args.get("where", "")
                    where_sql = where_clause.sql(dialect="duckdb") if where_clause else ""
                    
                    update_sql = f"UPDATE _target SET {set_clauses}"
                    if where_sql:
                        update_sql += f" {where_sql}"
                    
                    log.info(f"CoW UPDATE: {update_sql}")
                    local_conn.execute(update_sql)
                    affected = local_conn.execute("SELECT changes()").fetchone()[0]
                    
                elif isinstance(parsed, exp.Delete):
                    # Build DELETE on the local table  
                    where_clause = parsed.args.get("where", "")
                    where_sql = where_clause.sql(dialect="duckdb") if where_clause else ""
                    
                    delete_sql = "DELETE FROM _target"
                    if where_sql:
                        delete_sql += f" {where_sql}"
                    
                    log.info(f"CoW DELETE: {delete_sql}")
                    local_conn.execute(delete_sql)
                    affected = local_conn.execute("SELECT changes()").fetchone()[0]
                
                # Read back the mutated table
                result_arrow = local_conn.execute("SELECT * FROM _target").arrow()
                if hasattr(result_arrow, 'read_all'):
                    result_arrow = result_arrow.read_all()
                
                # Rename columns to match Iceberg schema
                iceberg_schema = iceberg_table.schema()
                expected_names = [field.name for field in iceberg_schema.fields]
                if len(expected_names) == len(result_arrow.column_names):
                    result_arrow = result_arrow.rename_columns(expected_names)
                
                # Overwrite the entire table (Copy-on-Write)
                iceberg_table.overwrite(result_arrow)
                
                op = "updated" if isinstance(parsed, exp.Update) else "deleted"
                return {"status": "ok", "msg": f"Success! {affected} rows {op} in '{target_table_name}' (Copy-on-Write).", "data": []}
            except Exception as e:
                log.error(f"CoW DML Transaction Failed: {e}")
                return {"error": f"DML Failed: {str(e)}"}
    except Exception as e:
        log.warning(f"AST V3 Rewrite warning: {e}. Falling back to raw execution.")
        
    # 4. Inject explicit Secure Tokens for Federated remote lakes
    if req.secrets:
        secret_stmts = []
        for s in req.secrets:
            s_name = s.get("name", "remote_db").replace("'", "")
            s_type = s.get("type", "POSTGRES").replace("'", "")
            s_host = s.get("host", "").replace("'", "''")
            s_user = s.get("user", "").replace("'", "''")
            s_pass = s.get("pass", "").replace("'", "''")
            s_port = str(s.get("port", "5432")).replace("'", "''")
            
            stmt = f"CREATE SECRET {s_name} (TYPE {s_type}, HOST '{s_host}', USER '{s_user}', PASSWORD '{s_pass}', PORT {s_port});"
            secret_stmts.append(stmt)
            
        req.sql = "\n".join(secret_stmts) + "\n" + req.sql
        
    # 5. Inject Global DuckPond Native REST Catalog dynamically
    tig_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    tig_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    tig_ep = tigris.TIGRIS_ENDPOINT.replace("https://", "")

    # Only include ATTACH for write operations that need the Iceberg catalog.
    # Read queries use iceberg_scan() directly, skipping the slow catalog ATTACH.
    needs_catalog = "enterprise_lake" in req.sql

    s3_secret = f"CREATE SECRET IF NOT EXISTS tigris_s3 (TYPE S3, KEY_ID '{tig_key}', SECRET '{tig_secret}', ENDPOINT '{tig_ep}');"

    if needs_catalog:
        iceberg_binds = f"""
    {s3_secret}
    CREATE SECRET IF NOT EXISTS iceberg_auth (TYPE ICEBERG, TOKEN '{os.environ.get("INTERNAL_AUTH_TOKEN", "duckpond-internal")}');
    ATTACH IF NOT EXISTS 'duckpond' AS enterprise_lake (
        TYPE ICEBERG,
        ENDPOINT 'https://duckpond-coordinator.fly.dev'
    );
    """
    else:
        iceberg_binds = f"\n    {s3_secret}\n    "

    req.sql = iceberg_binds + "\n" + req.sql
        
    if LOCAL_MODE:
        sp = get_sp()
        # For local smallpond parsing, if using presigned URLs, we might need a workaround,
        # but smallpond natively handles httpfs if loaded.
        result_df = sp.partial_sql(req.sql, None)
        table = result_df.to_arrow()
        res = _table_to_response(table, req.limit)
        res["elapsed_ms"] = round((time.time() - t0) * 1000)
        return res
    else:
        # Check timeout before dispatch
        elapsed = time.time() - t0
        if elapsed > QUERY_TIMEOUT_SECONDS:
            return {"error": f"Query timeout: processing exceeded {QUERY_TIMEOUT_SECONDS}s limit ({elapsed:.1f}s elapsed).", "data": []}
        
        log.info(f"Dispatching distributed query natively: tier={req.compute_tier}")
        return stream_via_fly_workers(req.sql, [], req.limit, req.output_mode, req.compute_tier)



@app.post("/ingest")
async def ingest(req: IngestRequest, user: dict = Depends(require_auth)):
    tenant_ns = user.get("namespace", "duckpond")
    
    tig_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    tig_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    tig_ep = tigris.TIGRIS_ENDPOINT.replace("https://", "")
    
    iceberg_binds = f"""
    CREATE SECRET IF NOT EXISTS tigris_s3 (TYPE S3, KEY_ID '{tig_key}', SECRET '{tig_secret}', ENDPOINT '{tig_ep}');
    CREATE SECRET IF NOT EXISTS iceberg_auth (TYPE ICEBERG, TOKEN '{os.environ.get("INTERNAL_AUTH_TOKEN", "duckpond-internal")}');
    ATTACH IF NOT EXISTS 'duckpond' AS enterprise_lake (
        TYPE ICEBERG,
        ENDPOINT 'https://duckpond-coordinator.fly.dev'
    );
    """
    
    sql = f"{iceberg_binds}\nDROP TABLE IF EXISTS enterprise_lake.{tenant_ns}.{req.table_name};\nCREATE TABLE enterprise_lake.{tenant_ns}.{req.table_name} AS SELECT * FROM read_parquet('{req.source_s3_uri}');"
    log.info(f"Dispatching ICEBERG Ingest ({req.compute_tier}): {req.source_s3_uri} -> {tenant_ns}.{req.table_name}")
    
    # [PHASE 3] Compute Quota Ledger Check for Ingest Workloads
    if not LOCAL_MODE and req.compute_tier != "micro" and "user" in locals() and user.get("id") != "system":
        try:
            db_res = auth_module.supabase.table("billing_profiles").select("id, compute_credit_balance, compute_rate").eq("owner_id", user.get("id")).execute()
            if db_res.data:
                profile = db_res.data[0]
                bal, rate = float(profile["compute_credit_balance"]), float(profile["compute_rate"])
                cost = rate if req.compute_tier == "distributed" else rate * 0.25
                if bal < cost:
                    return {"status": f"Error: Insufficient credits. Balance: ${bal:.3f}. Required: ${cost:.3f} for ingestion."}
                auth_module.supabase.table("billing_profiles").update({"compute_credit_balance": bal - cost}).eq("id", profile["id"]).execute()
        except Exception as e:
            pass

    try:
        resp = stream_via_fly_workers(sql, [], limit=100, output_mode="grid", compute_tier=req.compute_tier)
        final_msg = "Map-Reduce deployment completed silently."
        async for chunk_bytes in resp.body_iterator:
            chunk_str = chunk_bytes.decode('utf-8')
            log.info(f"Worker Output: {chunk_str}")
            try:
                msg = json.loads(chunk_str)
                if msg.get("type") == "error":
                    err_txt = msg.get('msg', msg.get('error', 'Unknown Error'))
                    return {"status": f"Worker crashed: {err_txt}"}
                if msg.get("msg"):
                    final_msg = msg.get("msg")
            except: pass
            
        return {"status": f"Ingestion success: {final_msg}"}
    except Exception as e:
        log.error(f"Worker ingest error: {e}")
        return {"status": f"Fatal Coordinator Error: {str(e)}"}

# ------------------------------------------------------------------------------
# [STRIPE BILLING ENGINE]
# ------------------------------------------------------------------------------
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "sk_test_mock")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "whsec_mock")

@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except ValueError as e:
        log.error("Invalid Stripe payload")
        raise HTTPException(status_code=400, detail="Invalid payload")
    except stripe.error.SignatureVerificationError as e:
        log.error("Invalid Stripe signature")
        raise HTTPException(status_code=400, detail="Invalid signature")

    # Handle successful checkout
    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        client_ref_id = session.get('client_reference_id') # Our mapped Supabase User ID
        amount_total = session.get('amount_total', 0) # e.g. 2450 for $24.50

        if not client_ref_id:
            log.warning("Detected successful checkout but no client_reference_id mapped.")
            return {"status": "ignored"}

        # Mathematical Upgrade Rules (Mock Mapping)
        plan_tier = 'pro'
        storage_limit = 100 * (1024 ** 3) # 100 GB default
        compute_rate = 0.028
        compute_credit_balance = 0.00
        
        # Determine strict tier upgrade via cost
        if amount_total >= 9900: # 99 USD
            plan_tier = 'team'
            storage_limit = 1024 * (1024 ** 3) # 1 TB
            compute_rate = 0.025
            compute_credit_balance = 20.00 # Base bucket of credits
        elif amount_total >= 2400: # 24 USD
            plan_tier = 'pro'
            storage_limit = 100 * (1024 ** 3) # 100 GB
            compute_rate = 0.028
            compute_credit_balance = 5.00 # Base bucket of credits
        elif amount_total >= 1250: # 12.50 USD
            plan_tier = 'starter'
            storage_limit = 50 * (1024 ** 3) # 50 GB default tier bound
            compute_rate = 0.028
            compute_credit_balance = 2.00 # Base bucket of credits
            
        log.info(f"STRIPE SUCCESS: Upgrading {client_ref_id} to {plan_tier.upper()}")
        
        try:
            # Wipe Trial, upgrade Storage Limit permanently, reset credits exactly via RPC/update
            auth_module.supabase.table("billing_profiles").update({
                "plan_tier": plan_tier,
                "trial_ends_at": None,
                "storage_limit_bytes": storage_limit,
                "compute_rate": compute_rate,
                "compute_credit_balance": compute_credit_balance
            }).eq("owner_id", client_ref_id).execute()
        except Exception as e:
            log.error(f"Failed to mathematically upgrade profile {client_ref_id} post-payment: {e}")
            return JSONResponse(status_code=500, content={"status": "database sync failed"})

    return JSONResponse(content={"status": "success"})


# -- Contact Form Email Proxy --------------------------------------------------
class ContactRequest(BaseModel):
    name: str
    email: str
    subject: str
    message: str

@app.post("/contact")
async def send_contact_email(req: ContactRequest):
    """Proxy for Resend email API — avoids CORS restrictions from browser."""
    RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
    payload = {
        "from": "onboarding@resend.dev",
        "to": "lorencogonzaga@gmail.com",
        "subject": f"[shikipond] {req.subject} — from {req.name}",
        "html": (
            f"<p><strong>From:</strong> {req.name} ({req.email})</p>"
            f"<p><strong>Subject:</strong> {req.subject}</p>"
            f"<hr/>"
            f"<p>{req.message.replace(chr(10), '<br/>')}</p>"
        ),
    }
    try:
        import urllib.request as _ur
        data = json.dumps(payload).encode("utf-8")
        r = _ur.Request(
            "https://api.resend.com/emails",
            data=data,
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with _ur.urlopen(r, timeout=15) as resp:
            body = json.loads(resp.read().decode())
        log.info(f"Contact email sent: {req.subject} from {req.email}")
        return JSONResponse(content={"status": "sent", "id": body.get("id")})
    except Exception as e:
        log.error(f"Contact email failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("coordinator:app", host="0.0.0.0", port=8081)
