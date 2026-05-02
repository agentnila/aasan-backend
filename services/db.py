"""
Supabase Postgres client — Tier 0 persistence layer.

Everything that wants to persist talks to Postgres through this module. When
SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY env vars are set, queries hit a real
Postgres connection pool. When unset (local dev, demo mode without a database),
calls return None and the calling service falls back to its in-memory store.

CONNECTION
──────────
Supabase exposes Postgres on port 5432 (transaction pooler at 6543, session at
5432). For Render → Supabase we use the **session pooler** (5432) since Render
is a long-running web process and prepared statements work cleanly there.

The full conn string is built from SUPABASE_URL — for a project ref `xxxx`,
the URL is `https://xxxx.supabase.co` and the Postgres host is
`db.xxxx.supabase.co`. Username is `postgres`, password is
SUPABASE_SERVICE_ROLE_KEY (or, if Balaji prefers, a separate
SUPABASE_DB_PASSWORD — both are checked).

WHY service_role key?
─────────────────────
The Render backend runs server-side and bypasses Row Level Security. Frontend
never gets this key. RLS isn't enabled in 0001_init.sql anyway (single-tenant
pilot), but using service_role keeps us future-proof for when it lands.

CONCURRENCY
───────────
psycopg3's ConnectionPool gives us a small pool (default min=1 max=5) suitable
for Render's free tier. Each request checks out a connection, runs the query,
returns it. No long-held transactions.

INTERFACE
─────────
  is_enabled() -> bool
      True when env vars are set AND a connection works. Cached.

  query(sql, params=None) -> list[dict] | None
      Read query. Returns rows as dicts. Returns None if not enabled.

  execute(sql, params=None) -> int | None
      Write query. Returns row count. Returns None if not enabled.

  execute_returning(sql, params=None) -> dict | None
      Write query that uses RETURNING. Returns one row as dict.

  transaction() -> contextmanager
      For multi-statement transactions. Use sparingly.

The `psycopg.sql` module is re-exported as `sql` for safe identifier composition
in callers that need it (e.g. dynamic table names in tests).

OBSERVABILITY
─────────────
Every query logs at debug level. Errors log at warning and re-raise. The
in-memory fallback path is silent — it's the normal local-dev experience.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Lazy psycopg import + pool init
# ──────────────────────────────────────────────────────────────

_pool = None
_enabled: bool | None = None  # tri-state: None=unknown, True=on, False=off
_psycopg = None
_pool_module = None


def _try_import_psycopg():
    global _psycopg, _pool_module
    if _psycopg is not None:
        return _psycopg
    try:
        import psycopg
        from psycopg_pool import ConnectionPool
        from psycopg.rows import dict_row
        _psycopg = psycopg
        _pool_module = ConnectionPool
        _psycopg._dict_row = dict_row  # type: ignore[attr-defined]
        return _psycopg
    except ImportError:
        logger.info("psycopg not installed — Supabase client disabled, services use in-memory fallback")
        return None


def _build_conninfo() -> str | None:
    """
    Build a Postgres conninfo string from env vars. Returns None if vars are
    missing — caller treats this as "fallback to in-memory."
    """
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    db_password = os.environ.get("SUPABASE_DB_PASSWORD", "").strip() or key

    if not url or not db_password:
        return None

    # Project ref is the subdomain of supabase.co
    # e.g. https://xxxx.supabase.co → xxxx
    if "://" in url:
        url_no_scheme = url.split("://", 1)[1]
    else:
        url_no_scheme = url
    host_parts = url_no_scheme.split(".")
    if len(host_parts) < 2:
        logger.warning("SUPABASE_URL %r doesn't look right — expected https://xxxx.supabase.co", url)
        return None
    project_ref = host_parts[0]

    # Allow override via SUPABASE_DB_HOST (e.g. for the pooler at aws-0-*.pooler.supabase.com)
    db_host = os.environ.get("SUPABASE_DB_HOST", "").strip() or f"db.{project_ref}.supabase.co"
    db_port = os.environ.get("SUPABASE_DB_PORT", "5432").strip()
    db_user = os.environ.get("SUPABASE_DB_USER", "").strip() or "postgres"
    db_name = os.environ.get("SUPABASE_DB_NAME", "").strip() or "postgres"
    sslmode = os.environ.get("SUPABASE_SSLMODE", "").strip() or "require"

    return (
        f"host={db_host} port={db_port} dbname={db_name} "
        f"user={db_user} password={db_password} sslmode={sslmode}"
    )


def _init_pool():
    global _pool, _enabled
    if _enabled is not None:
        return _enabled

    psycopg_mod = _try_import_psycopg()
    if psycopg_mod is None:
        _enabled = False
        return False

    conninfo = _build_conninfo()
    if conninfo is None:
        _enabled = False
        return False

    try:
        min_size = int(os.environ.get("SUPABASE_POOL_MIN", "1"))
        max_size = int(os.environ.get("SUPABASE_POOL_MAX", "5"))
        _pool = _pool_module(
            conninfo=conninfo,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": psycopg_mod._dict_row},  # type: ignore[attr-defined]
            open=True,
        )
        # Verify migrations have been applied — connect once and check sentinel
        with _pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT version FROM schema_migrations WHERE version = %s",
                ("0001_init",),
            )
            row = cur.fetchone()
            if not row:
                logger.warning(
                    "Connected to Supabase but migrations/0001_init.sql has not been run. "
                    "Run it via Supabase SQL editor before relying on persistence."
                )
        _enabled = True
        logger.info("Supabase Postgres connection pool initialized (min=%d max=%d)", min_size, max_size)
        return True
    except Exception as exc:
        logger.warning("Supabase Postgres init failed (%s) — services will use in-memory fallback", exc)
        _pool = None
        _enabled = False
        return False


def is_enabled() -> bool:
    """True if Supabase is reachable; False if env vars missing or connection failed."""
    if _enabled is None:
        _init_pool()
    return bool(_enabled)


def reset_for_tests():
    """Test helper — drop the cached pool so the next call re-initializes."""
    global _pool, _enabled
    if _pool is not None:
        try:
            _pool.close()
        except Exception:
            pass
    _pool = None
    _enabled = None


# ──────────────────────────────────────────────────────────────
# Query helpers
# ──────────────────────────────────────────────────────────────

def query(sql: str, params: Iterable[Any] | None = None) -> list[dict] | None:
    """
    Execute a SELECT and return rows as a list of dicts.
    Returns None if Supabase isn't available (caller falls back to in-memory).
    """
    if not is_enabled() or _pool is None:
        return None
    try:
        with _pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
    except Exception as exc:
        logger.warning("query failed: %s — sql=%s", exc, sql[:120])
        raise


def query_one(sql: str, params: Iterable[Any] | None = None) -> dict | None:
    """Execute a SELECT expected to return zero or one row."""
    rows = query(sql, params)
    if rows is None:
        return None
    return rows[0] if rows else None


def execute(sql: str, params: Iterable[Any] | None = None) -> int | None:
    """
    Execute a write (INSERT/UPDATE/DELETE) and return rowcount.
    Returns None if Supabase isn't available.
    """
    if not is_enabled() or _pool is None:
        return None
    try:
        with _pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())
            return cur.rowcount
    except Exception as exc:
        logger.warning("execute failed: %s — sql=%s", exc, sql[:120])
        raise


def execute_returning(sql: str, params: Iterable[Any] | None = None) -> dict | None:
    """
    Execute a write with RETURNING and fetch the single returned row.
    Returns None if Supabase isn't available.
    """
    if not is_enabled() or _pool is None:
        return None
    try:
        with _pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return row if row else None
    except Exception as exc:
        logger.warning("execute_returning failed: %s — sql=%s", exc, sql[:120])
        raise


def execute_many(sql: str, param_seq: list[Iterable[Any]]) -> int | None:
    """Bulk write. Returns total rowcount."""
    if not is_enabled() or _pool is None:
        return None
    if not param_seq:
        return 0
    try:
        with _pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(sql, param_seq)
            return cur.rowcount
    except Exception as exc:
        logger.warning("execute_many failed: %s — sql=%s", exc, sql[:120])
        raise


@contextmanager
def transaction():
    """
    Context manager for multi-statement transactions.

        with db.transaction() as cur:
            cur.execute("INSERT INTO goals ...")
            cur.execute("INSERT INTO paths ...")

    Yields a cursor. Auto-commits on clean exit, rolls back on exception.
    Returns None as the cursor when Supabase isn't available — caller must
    check and fall back.
    """
    if not is_enabled() or _pool is None:
        yield None
        return
    with _pool.connection() as conn:
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
