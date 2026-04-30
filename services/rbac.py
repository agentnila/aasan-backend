"""
RBAC — Role-Based Access Control for the Internal Pilot Pack (Job-1).

Phase 1 storage: in-memory `_USERS` dict keyed by user_id. Phase 2: Airtable
Users table or a real Postgres-backed user table. Demo seeds populate when
`demo-user` is the active id; everyone else gets a default `learner` row
on first access.

ROLE MODEL
──────────
  learner            — own data: Library, Paths, Stay Ahead, Resume, Marketplace
  manager            — above + Team module for direct reports
  skip_manager       — above + skip-level reports (read-only)
  ld_admin           — above + org-level skill heatmap + reporting + bulk ingest
  compliance_admin   — all learners' compliance status; cert tracking
  org_admin          — Admin Console (people, modules, branding, SSO)
  super_admin        — cross-org (multi-tenant; not V1)

Default role for SCIM-provisioned users: `learner`.
Demo special case: `demo-user` is treated as `org_admin` for the demo experience.

INTERFACE
─────────
  get_user(user_id) -> dict
  set_role(user_id, role) -> dict
  list_users() -> dict (manager pages — Phase 2 will paginate)
  has_role(user_id, *required_roles) -> bool
  has_any_permission(user_id, *permissions) -> bool
  user_can_view_module(user_id, module_id) -> bool
"""

from datetime import datetime


VALID_ROLES = {
    "learner", "manager", "skip_manager", "ld_admin",
    "compliance_admin", "org_admin", "super_admin",
}

# Module visibility per role. Modules NOT in the set are hidden in the rail
# AND any deep-link returns a 403 from the route guard. Order matters only
# for default sort.
ROLE_MODULES = {
    "learner":          {"kudil", "library", "paths", "stay-ahead", "resume", "marketplace"},
    "manager":          {"kudil", "library", "paths", "stay-ahead", "resume", "marketplace", "team"},
    "skip_manager":     {"kudil", "library", "paths", "stay-ahead", "resume", "marketplace", "team"},
    "ld_admin":         {"kudil", "library", "paths", "stay-ahead", "resume", "marketplace", "team"},
    "compliance_admin": {"kudil", "library", "paths", "stay-ahead", "resume", "marketplace", "team"},
    "org_admin":        {"kudil", "library", "paths", "stay-ahead", "resume", "marketplace", "team"},
    "super_admin":      {"kudil", "library", "paths", "stay-ahead", "resume", "marketplace", "team"},
}

# Role-level permissions (granular). Used by has_any_permission(...) on
# specific endpoints. The middleware below maps role → permissions on access.
ROLE_PERMISSIONS = {
    "learner":          {"goal:create_self", "path:edit_self", "resume:add_self"},
    "manager":          {"goal:create_self", "path:edit_self", "resume:add_self",
                         "team:view_reports", "team:send_kudos", "team:assign_learning"},
    "skip_manager":     {"goal:create_self", "path:edit_self", "resume:add_self",
                         "team:view_reports", "team:view_skip"},
    "ld_admin":         {"goal:create_self", "path:edit_self", "resume:add_self",
                         "team:view_all", "content:bulk_ingest", "report:run", "report:export"},
    "compliance_admin": {"goal:create_self", "path:edit_self", "resume:add_self",
                         "compliance:view_all", "compliance:assign_mandatory"},
    "org_admin":        {"admin:users", "admin:modules", "admin:branding",
                         "admin:sso", "admin:billing", "admin:audit_log",
                         "scim:provision"},
    "super_admin":      {"*"},
}


# ──────────────────────────────────────────────────────────────
# In-memory store. Phase 2: Postgres or Airtable Users table.
# ──────────────────────────────────────────────────────────────

_USERS = {}  # user_id → user dict


def _ensure_user(user_id: str) -> dict:
    """Return user record; auto-create with default role if missing."""
    if user_id not in _USERS:
        # Demo special case: demo-user is org_admin so the Admin Console is
        # exercisable on the canned product story.
        default_role = "org_admin" if user_id == "demo-user" else "learner"
        _USERS[user_id] = {
            "user_id": user_id,
            "email": user_id if "@" in user_id else f"{user_id}@example.com",
            "name": user_id.replace("-", " ").title() if user_id != "demo-user" else "Sarah Chen (demo)",
            "role": default_role,
            "department": "Platform Engineering" if user_id == "demo-user" else "",
            "manager_user_id": None,
            "is_active": True,
            "created_at": datetime.utcnow().isoformat(),
            "last_active_at": datetime.utcnow().isoformat(),
            "scim_external_id": None,
        }
        # Demo seed — populate the demo team as proper users so the Admin
        # Console "People" tab has something to render right away.
        if user_id == "demo-user":
            _seed_demo_team()
    return _USERS[user_id]


def _seed_demo_team():
    """Mirror the demo team in services/team.py as proper user records."""
    demo_team = [
        ("priya-singh",  "Priya Singh",  "priya.singh@example.com",  "manager",  "Platform Engineering"),
        ("david-kim",    "David Kim",    "david.kim@example.com",    "learner",  "Platform Engineering"),
        ("alex-rivera",  "Alex Rivera",  "alex.rivera@example.com",  "learner",  "Platform Engineering"),
        ("maya-patel",   "Maya Patel",   "maya.patel@example.com",   "learner",  "Platform Engineering"),
        ("jordan-lee",   "Jordan Lee",   "jordan.lee@example.com",   "learner",  "Platform Engineering"),
        # Add a couple of off-team people to make the People tab look populated
        ("raj-kumar",    "Raj Kumar",    "raj.kumar@example.com",    "ld_admin", "People & Learning"),
        ("legal-contact","Legal Contact","compliance@example.com",   "compliance_admin", "Legal & Compliance"),
    ]
    now = datetime.utcnow().isoformat()
    for uid, name, email, role, dept in demo_team:
        if uid in _USERS:
            continue
        _USERS[uid] = {
            "user_id": uid, "email": email, "name": name, "role": role,
            "department": dept, "manager_user_id": "demo-user",
            "is_active": True, "created_at": now, "last_active_at": now,
            "scim_external_id": None,
        }


# ──────────────────────────────────────────────────────────────
# Public API — read
# ──────────────────────────────────────────────────────────────

def get_user(user_id: str) -> dict:
    return dict(_ensure_user(user_id))


def get_role(user_id: str) -> str:
    return _ensure_user(user_id)["role"]


def has_role(user_id: str, *required: str) -> bool:
    role = get_role(user_id)
    if role == "super_admin":
        return True
    return role in set(required)


def has_any_permission(user_id: str, *required: str) -> bool:
    role = get_role(user_id)
    perms = ROLE_PERMISSIONS.get(role, set())
    if "*" in perms:
        return True
    return any(p in perms for p in required)


def user_can_view_module(user_id: str, module_id: str) -> bool:
    role = get_role(user_id)
    return module_id in ROLE_MODULES.get(role, set())


def me(user_id: str) -> dict:
    """Returns the active user's identity + role + module visibility for UI gating."""
    u = _ensure_user(user_id)
    return {
        "user_id": u["user_id"],
        "email": u["email"],
        "name": u["name"],
        "role": u["role"],
        "department": u.get("department", ""),
        "manager_user_id": u.get("manager_user_id"),
        "modules": sorted(ROLE_MODULES.get(u["role"], set())),
        "permissions": sorted(ROLE_PERMISSIONS.get(u["role"], set())),
        "is_admin": u["role"] in ("org_admin", "super_admin"),
    }


def list_users(filter_role: str = None, search: str = None, limit: int = 200) -> dict:
    # Make sure the demo seed has been triggered at least once
    _ensure_user("demo-user")
    users = list(_USERS.values())
    if filter_role:
        users = [u for u in users if u.get("role") == filter_role]
    if search:
        s = search.lower()
        users = [
            u for u in users
            if s in (u.get("name") or "").lower() or s in (u.get("email") or "").lower()
        ]
    users.sort(key=lambda u: (u.get("role") == "org_admin") and -1 or 0)  # admins first
    by_role = {}
    for u in _USERS.values():
        r = u.get("role", "learner")
        by_role[r] = by_role.get(r, 0) + 1
    return {
        "users": users[:limit],
        "count": len(users),
        "total": len(_USERS),
        "by_role": by_role,
    }


# ──────────────────────────────────────────────────────────────
# Public API — write
# ──────────────────────────────────────────────────────────────

def set_role(actor_user_id: str, target_user_id: str, new_role: str) -> dict:
    if new_role not in VALID_ROLES:
        return {"error": f"role must be one of {sorted(VALID_ROLES)}"}
    if not has_any_permission(actor_user_id, "admin:users"):
        return {"error": "forbidden — only org_admin can change roles"}
    target = _ensure_user(target_user_id)
    target["role"] = new_role
    target["updated_at"] = datetime.utcnow().isoformat()
    return {"ok": True, "user": target}


def update_user(actor_user_id: str, target_user_id: str, fields: dict) -> dict:
    if not has_any_permission(actor_user_id, "admin:users"):
        return {"error": "forbidden — only org_admin can update users"}
    target = _ensure_user(target_user_id)
    for k in ("name", "email", "department", "manager_user_id", "is_active"):
        if k in fields:
            target[k] = fields[k]
    target["updated_at"] = datetime.utcnow().isoformat()
    return {"ok": True, "user": target}


# ──────────────────────────────────────────────────────────────
# Flask middleware helper
# ──────────────────────────────────────────────────────────────

def get_actor_user_id(req) -> str:
    """
    Extract the actor's user_id from a Flask request. Phase 1: trusts the
    `X-Aasan-User` header OR `actor_user_id` in the JSON body. Phase 2:
    parse the Clerk JWT from `Authorization: Bearer ...`.
    """
    uid = req.headers.get("X-Aasan-User")
    if uid:
        return uid.strip()
    try:
        body = req.get_json(silent=True) or {}
        return (body.get("actor_user_id") or body.get("user_id") or "demo-user").strip()
    except Exception:
        return "demo-user"


def require_permission(*permissions):
    """
    Flask decorator. Wraps a view function with a permission check.
    Usage:
        @app.route(...)
        @require_permission("admin:users")
        def my_route():
            ...
    """
    from functools import wraps
    from flask import jsonify, request as _flask_request

    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            actor = get_actor_user_id(_flask_request)
            if not has_any_permission(actor, *permissions):
                return jsonify({
                    "error": "forbidden",
                    "required_any": list(permissions),
                    "your_role": get_role(actor),
                }), 403
            return fn(*args, **kwargs)
        return wrapper
    return decorator
