"""
DuckPond Auth Module — Supabase JWT verification and tenant namespace resolution.
"""
import os
import logging
import urllib.request
import json

log = logging.getLogger("coordinator.auth")

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://jmexnqbrwrzxbfysrleh.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")


def verify_token(token: str) -> dict | None:
    """
    Verify a Supabase access token by calling the GoTrue /auth/v1/user endpoint.
    Returns the user dict on success, None on failure.
    This approach doesn't require the JWT secret — Supabase validates it server-side.
    """
    try:
        req = urllib.request.Request(f"{SUPABASE_URL}/auth/v1/user")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("apikey", SUPABASE_ANON_KEY)
        with urllib.request.urlopen(req, timeout=5) as resp:
            user = json.loads(resp.read().decode())
            return user
    except urllib.error.HTTPError as e:
        log.warning(f"Auth verification failed: {e.code}")
        return None
    except Exception as e:
        log.error(f"Auth verification error: {e}")
        return None


def get_tenant_namespace(user: dict) -> str:
    """
    Resolve the tenant namespace from a verified Supabase user.
    
    - If the user belongs to an organization (org_id in app_metadata), 
      use org_{slug} as the namespace.
    - Otherwise, use t_{user_id} (individual tenant).
    """
    app_meta = user.get("app_metadata", {})
    
    # Check for organization membership
    org_id = app_meta.get("org_id")
    org_slug = app_meta.get("org_slug")
    if org_id and org_slug:
        return f"org_{org_slug}"
    
    # Individual user — namespace by user ID
    user_id = user.get("id", "unknown")
    # Use first 12 chars of UUID for readability
    short_id = user_id.replace("-", "")[:12]
    return f"t_{short_id}"


def get_user_display(user: dict) -> dict:
    """Extract display-friendly user info."""
    return {
        "id": user.get("id"),
        "email": user.get("email"),
        "namespace": get_tenant_namespace(user),
    }


# ---------------------------------------------------------------------------
# Lightweight Supabase REST client (no extra SDK dependency)
# ---------------------------------------------------------------------------

class _QueryBuilder:
    def __init__(self, url: str, service_key: str, anon_key: str, table: str, method: str):
        self._url = url
        self._service_key = service_key
        self._anon_key = anon_key
        self._table = table
        self._method = method
        self._filters: list[str] = []
        self._select_cols = "*"
        self._body: dict | None = None

    def eq(self, col: str, val: str) -> "_QueryBuilder":
        self._filters.append(f"{col}=eq.{val}")
        return self

    def select(self, cols: str) -> "_QueryBuilder":
        self._select_cols = cols
        return self

    def execute(self):
        params = "&".join(self._filters)
        if self._method == "GET":
            params += f"&select={self._select_cols}" if self._filters else f"select={self._select_cols}"
        endpoint = f"{self._url}/rest/v1/{self._table}?{params}"
        data = json.dumps(self._body).encode() if self._body else None
        req = urllib.request.Request(endpoint, data=data, method=self._method)
        req.add_header("Authorization", f"Bearer {self._service_key}")
        req.add_header("apikey", self._service_key)
        req.add_header("Content-Type", "application/json")
        req.add_header("Prefer", "return=representation")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = json.loads(resp.read().decode())
                return type("Result", (), {"data": body, "error": None})()
        except urllib.error.HTTPError as e:
            err = e.read().decode()
            log.error(f"Supabase REST error {e.code}: {err}")
            return type("Result", (), {"data": [], "error": err})()


class _TableProxy:
    def __init__(self, url: str, service_key: str, anon_key: str, table: str):
        self._url = url
        self._sk = service_key
        self._ak = anon_key
        self._table = table

    def select(self, cols: str = "*") -> _QueryBuilder:
        b = _QueryBuilder(self._url, self._sk, self._ak, self._table, "GET")
        b._select_cols = cols
        return b

    def insert(self, body: dict) -> _QueryBuilder:
        b = _QueryBuilder(self._url, self._sk, self._ak, self._table, "POST")
        b._body = body
        return b

    def update(self, body: dict) -> _QueryBuilder:
        b = _QueryBuilder(self._url, self._sk, self._ak, self._table, "PATCH")
        b._body = body
        return b

    def delete(self) -> _QueryBuilder:
        return _QueryBuilder(self._url, self._sk, self._ak, self._table, "DELETE")


class _SupabaseClient:
    def __init__(self):
        self._url = SUPABASE_URL
        self._sk = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
        self._ak = SUPABASE_ANON_KEY

    def table(self, name: str) -> _TableProxy:
        return _TableProxy(self._url, self._sk, self._ak, name)


supabase = _SupabaseClient()
