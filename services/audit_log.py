"""
Audit log — Internal Pilot Pack · Phase C.

Immutable record of who did what, when. Wraps state-changing endpoints
via the @audit decorator. Phase 1 storage: in-memory `_LOG` list ordered
oldest → newest. Phase 2: Postgres-backed (append-only, no UPDATE/DELETE)
once we have a real DB. Foundation for SOC 2 even though we're not in
formal audit yet.

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
import re
from datetime import datetime
from functools import wraps


_LOG = []
_SEQ = [0]


def record(actor_user_id: str, action: str, target: str = None, details: dict = None,
           actor_role: str = None, request_id: str = None) -> dict:
    """Append an audit entry. Idempotent on (actor, action, target, ms-bucket)
    is NOT enforced — duplicate calls produce duplicate rows by design."""
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
    _LOG.append(entry)
    return entry


# ──────────────────────────────────────────────────────────────
# Query / search
# ──────────────────────────────────────────────────────────────

def query(filter_actor: str = None, filter_action: str = None, filter_target: str = None,
          since: str = None, until: str = None, search: str = None,
          limit: int = 200) -> dict:
    """Newest-first; filters AND-combined."""
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
