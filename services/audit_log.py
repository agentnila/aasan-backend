"""
Audit log — Internal Pilot Pack · Phase C.

Immutable record of who did what, when. Wraps state-changing endpoints
via the @audit decorator. Foundation for SOC 2 even though we're not in
formal audit yet.

STORAGE
───────
Dual-mode (Tier 0, 2026-05-01):
  - When SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY env vars are set, every
    record() writes to the Postgres `audit_log` table. query() reads from it.
  - When env vars are absent, the in-memory `_LOG` list is the source of
    truth. Lossy on Render restart but fine for local dev / demo.
The Postgres path is append-only — no UPDATE or DELETE happens from app code.

ENTRY SHAPE
───────────
  {
    audit_id:     "a-{epoch_ms}-{seq}"   (string, unique)
    timestamp:    ISO 8601 UTC
    actor_user_id: str
    actor_role:    str  (snapshot at time of action)
    action:        str  (e.g. "admin:role_change", "goal:create")
    target:        str  (the object affected, e.g. "user:jordan-lee")
    details:       dict (free-form context — old value, new value, etc.)
    request_id:    str  (optional — for tracing)
  }

DESIGN NOTES
────────────
- Records ONLY mutating events. Reads (list users, view path) don't audit
  by default — would explode the log. Future: per-customer toggle for
  read-audit (compliance industries may need it).
- Failures DON'T audit. The wrapper records only when the wrapped function
  returned a 2xx response. Failed-permission errors are interesting but
  noisy; we'll add a separate access_denied log later if needed.
- The `details` field is intentionally free-form. Each action type
  decides what's worth recording.
"""

import csv
import io
import json
import logging
import re
from datetime import datetime
from functools import wraps

from . import db

logger = logging.getLogger(__name__)


_LOG = []
_SEQ = [0]


def record(actor_user_id: str, action: str, target: str = None, details: dict = None,
           actor_role: str = None, request_id: str = None) -> dict:
    """Append an audit entry. Writes through to Postgres when configured;
    always also appends to in-memory _LOG so the same-process query path
    sees the entry without a round-trip. Idempotency is NOT enforced —
    duplicate calls produce duplicate rows by design."""
    _SEQ[0] += 1
    ts = datetime.utcnow()
    entry = {
        "audit_id": f"a-{int(ts.timestamp() * 1000)}-{_SEQ[0]}",
        "timestamp": ts.isoformat(),
        "actor_user_id": actor_user_id or "unknown",
        "actor_role": actor_role,
        "action": action,
        "target": target or "",
        "details": details or {},
        "request_id": request_id,
    }

    # Write through to Postgres (best effort — never block on audit failures)
    if db.is_enabled():
        try:
            db.execute(
                """
                INSERT INTO audit_log
                    (audit_id, occurred_at, actor_user_id, actor_role,
                     action, target, details, request_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (audit_id) DO NOTHING
                """,
                (
                    entry["audit_id"],
                    ts,
                    entry["actor_user_id"],
                    entry["actor_role"],
                    entry["action"],
                    entry["target"],
                    json.dumps(entry["details"]),
                    entry["request_id"],
                ),
            )
        except Exception as exc:
            logger.warning("audit_log write to Postgres failed (%s) — recorded in memory only", exc)

    _LOG.append(entry)
    return entry


# ──────────────────────────────────────────────────────────────
# Query / search
# ──────────────────────────────────────────────────────────────

def query(filter_actor: str = None, filter_action: str = None, filter_target: str = None,
          since: str = None, until: str = None, search: str = None,
          limit: int = 200) -> dict:
    """Newest-first; filters AND-combined.

    When Postgres is configured, reads from the `audit_log` table directly
    (filters pushed into SQL where possible). Otherwise filters in-memory
    `_LOG`. Aggregate stats (by_action / by_actor) come from the full log,
    not the filtered subset, so callers see total volume even when scoped.
    """
    if db.is_enabled():
        try:
            return _query_pg(
                filter_actor=filter_actor, filter_action=filter_action,
                filter_target=filter_target, since=since, until=until,
                search=search, limit=limit,
            )
        except Exception as exc:
            logger.warning("audit_log query from Postgres failed (%s) — falling back to in-memory", exc)

    out = list(reversed(_LOG))

    if filter_actor:
        a = filter_actor.lower()
        out = [e for e in out if a in (e.get("actor_user_id") or "").lower()]

    if filter_action:
        # Glob support: "admin:*" matches any admin action
        if filter_action.endswith("*"):
            prefix = filter_action[:-1]
            out = [e for e in out if (e.get("action") or "").startswith(prefix)]
        else:
            out = [e for e in out if e.get("action") == filter_action]

    if filter_target:
        t = filter_target.lower()
        out = [e for e in out if t in (e.get("target") or "").lower()]

    if since:
        out = [e for e in out if (e.get("timestamp") or "") >= since]
    if until:
        out = [e for e in out if (e.get("timestamp") or "") <= until]

    if search:
        s = search.lower()
        def _hit(e):
            blob = " ".join([
                e.get("actor_user_id", ""), e.get("action", ""),
                e.get("target", ""), str(e.get("details", "")),
            ]).lower()
            return s in blob
        out = [e for e in out if _hit(e)]

    # Aggregate stats over the FULL log (not the filtered subset)
    actions = {}
    actors = {}
    for e in _LOG:
        actions[e.get("action", "")] = actions.get(e.get("action", ""), 0) + 1
        actors[e.get("actor_user_id", "")] = actors.get(e.get("actor_user_id", ""), 0) + 1

    return {
        "entries": out[:limit],
        "filtered_count": len(out),
        "total": len(_LOG),
        "by_action": dict(sorted(actions.items(), key=lambda kv: -kv[1])[:20]),
        "by_actor":  dict(sorted(actors.items(),  key=lambda kv: -kv[1])[:20]),
    }


def _query_pg(filter_actor=None, filter_action=None, filter_target=None,
              since=None, until=None, search=None, limit=200) -> dict:
    """Postgres-backed implementation of query()."""
    clauses: list[str] = []
    params: list = []

    if filter_actor:
        clauses.append("LOWER(actor_user_id) LIKE %s")
        params.append(f"%{filter_actor.lower()}%")

    if filter_action:
        if filter_action.endswith("*"):
            clauses.append("action LIKE %s")
            params.append(f"{filter_action[:-1]}%")
        else:
            clauses.append("action = %s")
            params.append(filter_action)

    if filter_target:
        clauses.append("LOWER(target) LIKE %s")
        params.append(f"%{filter_target.lower()}%")

    if since:
        clauses.append("occurred_at >= %s")
        params.append(since)
    if until:
        clauses.append("occurred_at <= %s")
        params.append(until)

    if search:
        clauses.append(
            "(LOWER(actor_user_id) LIKE %s "
            "OR LOWER(action) LIKE %s "
            "OR LOWER(COALESCE(target, '')) LIKE %s "
            "OR LOWER(details::text) LIKE %s)"
        )
        s = f"%{search.lower()}%"
        params.extend([s, s, s, s])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))
    rows = db.query(
        f"""
        SELECT audit_id, occurred_at, actor_user_id, actor_role,
               action, target, details, request_id
        FROM audit_log
        {where}
        ORDER BY occurred_at DESC
        LIMIT %s
        """,
        params,
    ) or []

    entries = [_pg_row_to_entry(r) for r in rows]

    # filtered_count needs the COUNT(*) for the same WHERE
    if clauses:
        count_row = db.query_one(
            f"SELECT COUNT(*) AS n FROM audit_log {where}",
            params[:-1],  # drop the LIMIT param
        )
        filtered_count = int(count_row["n"]) if count_row else len(entries)
    else:
        filtered_count = len(entries)

    total_row = db.query_one("SELECT COUNT(*) AS n FROM audit_log")
    total = int(total_row["n"]) if total_row else 0

    by_action = {
        r["action"]: int(r["n"])
        for r in (db.query(
            "SELECT action, COUNT(*) AS n FROM audit_log GROUP BY action ORDER BY n DESC LIMIT 20"
        ) or [])
    }
    by_actor = {
        r["actor_user_id"]: int(r["n"])
        for r in (db.query(
            "SELECT actor_user_id, COUNT(*) AS n FROM audit_log GROUP BY actor_user_id ORDER BY n DESC LIMIT 20"
        ) or [])
    }

    return {
        "entries": entries,
        "filtered_count": filtered_count,
        "total": total,
        "by_action": by_action,
        "by_actor": by_actor,
    }


def _pg_row_to_entry(row: dict) -> dict:
    """Normalize a Postgres audit_log row to the in-memory entry shape."""
    occurred = row.get("occurred_at")
    ts = occurred.isoformat() if hasattr(occurred, "isoformat") else (occurred or "")
    details = row.get("details")
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except (ValueError, TypeError):
            details = {}
    elif details is None:
        details = {}
    return {
        "audit_id": row.get("audit_id"),
        "timestamp": ts,
        "actor_user_id": row.get("actor_user_id") or "unknown",
        "actor_role": row.get("actor_role"),
        "action": row.get("action") or "",
        "target": row.get("target") or "",
        "details": details,
        "request_id": row.get("request_id"),
    }


# ──────────────────────────────────────────────────────────────
# Export
# ──────────────────────────────────────────────────────────────

def export_csv(filters: dict = None) -> str:
    """Return CSV string of (filtered) entries."""
    filtered = query(
        filter_actor=(filters or {}).get("filter_actor"),
        filter_action=(filters or {}).get("filter_action"),
        filter_target=(filters or {}).get("filter_target"),
        since=(filters or {}).get("since"),
        until=(filters or {}).get("until"),
        search=(filters or {}).get("search"),
        limit=10000,  # CSV exports go higher than UI list
    )["entries"]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["audit_id", "timestamp", "actor_user_id", "actor_role", "action", "target", "details"])
    for e in filtered:
        writer.writerow([
            e.get("audit_id", ""), e.get("timestamp", ""), e.get("actor_user_id", ""),
            e.get("actor_role") or "", e.get("action", ""), e.get("target", ""),
            (str(e.get("details") or "")).replace("\n", " "),
        ])
    return buf.getvalue()


# ──────────────────────────────────────────────────────────────
# Decorator — @audit_action("admin:role_change", target_fn=...)
# ──────────────────────────────────────────────────────────────

def audit_action(action: str, target_fn=None, details_fn=None):
    """
    Wrap a Flask view function. After the wrapped function returns a 2xx
    response, append an audit entry.

    Usage:
        @app.route("/admin/users/set_role", methods=["POST"])
        @audit_action(
            "admin:role_change",
            target_fn=lambda req, resp: f"user:{req.json.get('target_user_id','?')}",
            details_fn=lambda req, resp: {"new_role": req.json.get('role')},
        )
        def admin_users_set_role():
            ...

    target_fn / details_fn each receive (request, response_tuple) where
    response_tuple is (jsonified_response, status_code) or just response.
    """
    from flask import request as _req

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            response = fn(*args, **kwargs)
            # Normalize Flask response forms
            status = 200
            body_obj = response
            if isinstance(response, tuple):
                body_obj, status = response[0], response[1] if len(response) > 1 else 200
            try:
                # Only audit successful state changes
                if 200 <= int(status) < 300:
                    # Lazy-import rbac to avoid circular at module load
                    from . import rbac as _rbac
                    actor = _rbac.get_actor_user_id(_req)
                    actor_role = _rbac.get_role(actor) if actor else None
                    target = target_fn(_req, response) if target_fn else ""
                    details = details_fn(_req, response) if details_fn else {}
                    record(
                        actor_user_id=actor, action=action,
                        target=target, details=details, actor_role=actor_role,
                    )
            except Exception as exc:
                # Never let audit failure break the actual call
                print(f"[audit_log] failed to record {action}: {exc}")
            return response
        return wrapper
    return decorator


# ──────────────────────────────────────────────────────────────
# Convenience: structured target builders for common shapes
# ──────────────────────────────────────────────────────────────

def target_user(req, _resp=None):
    body = req.get_json(silent=True) or {}
    return f"user:{body.get('target_user_id') or body.get('user_id') or '?'}"


def target_goal(req, _resp=None):
    body = req.get_json(silent=True) or {}
    goal = body.get("goal") or {}
    return f"goal:{(goal.get('id') or goal.get('name') or body.get('goal_id') or '?')}"


def target_path_step(req, _resp=None):
    body = req.get_json(silent=True) or {}
    return f"step:{body.get('step_id', '?')}@goal:{body.get('goal_id', '?')}"


def target_resume_entry(req, _resp=None):
    body = req.get_json(silent=True) or {}
    return f"entry:{body.get('entry_id', '?')}"
