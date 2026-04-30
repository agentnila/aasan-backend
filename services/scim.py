"""
SCIM 2.0 provisioning — Internal Pilot Pack · Phase G.

Okta-compatible System for Cross-domain Identity Management endpoints.
Lets the customer's IdP (Okta / Azure AD / Google Workspace / Okta-on-prem)
push user lifecycle events directly to Aasan: create, update, activate,
deactivate, deprovision. No more CSV uploads on day-of-onboarding.

WHAT'S IMPLEMENTED (V1 — covers Okta's "Push profile updates" + "Push
deactivation" + "Import users" use cases):

   GET  /scim/v2/ServiceProviderConfig   — what we support
   GET  /scim/v2/ResourceTypes           — what kind of resources we expose
   GET  /scim/v2/Schemas                 — schemas (User + EnterpriseUser ext)
   GET  /scim/v2/Users                   — list (with filter, pagination)
   POST /scim/v2/Users                   — create user
   GET  /scim/v2/Users/<id>              — read user
   PUT  /scim/v2/Users/<id>              — replace user
   PATCH /scim/v2/Users/<id>             — partial update (Okta's deactivate)
   DELETE /scim/v2/Users/<id>            — hard deprovision (rare; usually PATCH active=false)

AUTH:
   Bearer token in Authorization header. Token is opaque and lives in
   _SCIM_TOKENS (issued by org_admin via /admin/scim/issue_token).
   Phase 2 will rotate tokens + scope them per IdP.

MAPPING (SCIM User → Aasan rbac user dict):
   userName              → email
   externalId            → scim_external_id (the IdP's stable ID)
   name.formatted        → name
   active                → is_active
   emails[primary=true]  → email (also)
   title                 → job_role (mapped to onboarding template)
   urn:ietf:params:scim:schemas:extension:enterprise:2.0:User:
     department          → department
     manager.value       → manager_user_id (resolved by externalId or email)

ON CREATE: triggers onboarding template apply (same hook as CSV import).
ON DEACTIVATE: sets is_active=false, role='learner' (drops admin perms).
ON DELETE: sets is_active=false but does NOT remove the row (so audit
log + Resume entries stay attached to a real identity).

Phase 2 storage: Airtable Tables 30 (SCIM_Tokens) + 31 (SCIM_Sync_Log).
"""

import secrets
from datetime import datetime


# ──────────────────────────────────────────────────────────────
# Token store (in-memory; one tenant for V1)
# ──────────────────────────────────────────────────────────────

_SCIM_TOKENS = {}  # token → {issued_by, issued_at, label, last_used_at}
_SCIM_SYNC_LOG = []  # list of {ts, action, target_id, source}


SCIM_USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User"
SCIM_ENTERPRISE_EXT = "urn:ietf:params:scim:schemas:extension:enterprise:2.0:User"
SCIM_LIST_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
SCIM_PATCH_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
SCIM_ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"


# ──────────────────────────────────────────────────────────────
# Token management
# ──────────────────────────────────────────────────────────────

def issue_token(actor_user_id: str, label: str = "") -> dict:
    """Issue a new SCIM bearer token. Caller must have admin:sso permission."""
    from . import rbac as _rbac
    if not _rbac.has_any_permission(actor_user_id, "admin:sso"):
        return {"error": "forbidden — admin:sso required"}
    token = "scim_" + secrets.token_urlsafe(32)
    _SCIM_TOKENS[token] = {
        "issued_by": actor_user_id,
        "issued_at": datetime.utcnow().isoformat(),
        "label": label or "Default SCIM token",
        "last_used_at": None,
        "use_count": 0,
    }
    return {"ok": True, "token": token, "label": _SCIM_TOKENS[token]["label"]}


def list_tokens(actor_user_id: str) -> dict:
    """List SCIM tokens (preview only — full token shown only at creation)."""
    from . import rbac as _rbac
    if not _rbac.has_any_permission(actor_user_id, "admin:sso"):
        return {"error": "forbidden"}
    return {
        "tokens": [
            {
                "preview": f"{tok[:14]}…{tok[-4:]}",
                "label": meta["label"],
                "issued_by": meta["issued_by"],
                "issued_at": meta["issued_at"],
                "last_used_at": meta["last_used_at"],
                "use_count": meta["use_count"],
            }
            for tok, meta in _SCIM_TOKENS.items()
        ],
    }


def revoke_token(actor_user_id: str, token_preview: str) -> dict:
    from . import rbac as _rbac
    if not _rbac.has_any_permission(actor_user_id, "admin:sso"):
        return {"error": "forbidden"}
    for tok in list(_SCIM_TOKENS.keys()):
        if tok.startswith(token_preview.rstrip("…").split("…")[0]):
            del _SCIM_TOKENS[tok]
            return {"ok": True, "revoked": True}
    return {"error": "token not found"}


def verify_bearer(req) -> bool:
    """Validate the Authorization header for SCIM endpoints."""
    auth = req.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    token = auth[7:].strip()
    meta = _SCIM_TOKENS.get(token)
    if not meta:
        return False
    meta["last_used_at"] = datetime.utcnow().isoformat()
    meta["use_count"] = meta.get("use_count", 0) + 1
    return True


def get_sync_log(limit: int = 50) -> list:
    return list(reversed(_SCIM_SYNC_LOG[-limit:]))


def _log_sync(action: str, target_id: str, source: str = "scim"):
    _SCIM_SYNC_LOG.append({
        "ts": datetime.utcnow().isoformat(),
        "action": action,
        "target_id": target_id,
        "source": source,
    })


# ──────────────────────────────────────────────────────────────
# SCIM ⇄ Aasan user dict mapping
# ──────────────────────────────────────────────────────────────

def to_scim(user: dict) -> dict:
    """Convert an Aasan rbac user dict into a SCIM User resource."""
    parts = (user.get("name") or "").split(" ", 1)
    given = parts[0] if parts else ""
    family = parts[1] if len(parts) > 1 else ""
    return {
        "schemas": [SCIM_USER_SCHEMA, SCIM_ENTERPRISE_EXT],
        "id": user["user_id"],
        "externalId": user.get("scim_external_id"),
        "userName": user.get("email") or user["user_id"],
        "name": {
            "formatted": user.get("name", ""),
            "givenName": given,
            "familyName": family,
        },
        "displayName": user.get("name", ""),
        "active": bool(user.get("is_active", True)),
        "emails": [
            {"value": user.get("email", ""), "primary": True, "type": "work"}
        ] if user.get("email") else [],
        "title": user.get("job_role") or "",
        "userType": user.get("role", "learner"),
        SCIM_ENTERPRISE_EXT: {
            "department": user.get("department", ""),
            "manager": (
                {"value": user["manager_user_id"]}
                if user.get("manager_user_id") else None
            ),
        },
        "meta": {
            "resourceType": "User",
            "created": user.get("created_at"),
            "lastModified": user.get("updated_at") or user.get("last_active_at"),
            "location": f"/scim/v2/Users/{user['user_id']}",
        },
    }


def _flatten_scim_payload(payload: dict) -> dict:
    """
    Pull the fields we care about out of an arbitrary SCIM User resource.
    Returns a dict with keys: email, name, active, scim_external_id,
    department, manager_email_or_id, job_role, role.
    """
    flat = {}
    flat["scim_external_id"] = payload.get("externalId")
    flat["email"] = (payload.get("userName") or "").strip().lower()
    if not flat["email"]:
        # Try emails[primary] fallback
        for em in payload.get("emails") or []:
            if em.get("primary") and em.get("value"):
                flat["email"] = em["value"].strip().lower()
                break

    name = payload.get("name") or {}
    formatted = name.get("formatted")
    if formatted:
        flat["name"] = formatted.strip()
    else:
        flat["name"] = (
            f"{name.get('givenName', '').strip()} {name.get('familyName', '').strip()}".strip()
            or payload.get("displayName")
            or ""
        )

    if "active" in payload:
        flat["active"] = bool(payload["active"])

    flat["job_role"] = (payload.get("title") or "").strip().lower() or None

    # Aasan role can come from userType OR a custom roles array
    user_type = (payload.get("userType") or "").strip().lower()
    from . import rbac as _rbac
    if user_type and user_type in _rbac.VALID_ROLES:
        flat["role"] = user_type

    ent = payload.get(SCIM_ENTERPRISE_EXT) or {}
    flat["department"] = (ent.get("department") or "").strip()
    mgr = ent.get("manager") or {}
    if isinstance(mgr, dict):
        flat["manager_ref"] = mgr.get("value") or mgr.get("$ref") or mgr.get("displayName")
    elif isinstance(mgr, str):
        flat["manager_ref"] = mgr
    return flat


def _resolve_manager(ref: str) -> str:
    """Manager ref might be a user_id, an email, or an externalId. Try each."""
    if not ref:
        return None
    from . import rbac as _rbac
    # 1. Direct user_id?
    if ref in _rbac._USERS:
        return ref
    # 2. Email match?
    for u in _rbac._USERS.values():
        if (u.get("email") or "").lower() == ref.lower():
            return u["user_id"]
    # 3. externalId match?
    for u in _rbac._USERS.values():
        if u.get("scim_external_id") == ref:
            return u["user_id"]
    return None


# ──────────────────────────────────────────────────────────────
# CRUD operations (called from Flask views)
# ──────────────────────────────────────────────────────────────

def list_users(scim_filter: str = None, start_index: int = 1, count: int = 100) -> dict:
    """
    GET /scim/v2/Users — Okta calls this with ?filter=userName eq "alice@x.com".
    We support exact-match `userName eq "..."` and `externalId eq "..."`.
    """
    from . import rbac as _rbac
    _rbac._ensure_user("demo-user")  # trigger seed
    users = list(_rbac._USERS.values())

    if scim_filter:
        match = _parse_eq_filter(scim_filter)
        if match:
            field, value = match
            if field.lower() == "username":
                users = [u for u in users if (u.get("email") or "").lower() == value.lower()]
            elif field.lower() == "externalid":
                users = [u for u in users if u.get("scim_external_id") == value]
            elif field.lower() == "active":
                want = value.lower() == "true"
                users = [u for u in users if bool(u.get("is_active", True)) == want]

    total = len(users)
    start = max(1, int(start_index)) - 1
    page = users[start:start + int(count)]

    return {
        "schemas": [SCIM_LIST_SCHEMA],
        "totalResults": total,
        "startIndex": start_index,
        "itemsPerPage": len(page),
        "Resources": [to_scim(u) for u in page],
    }


def _parse_eq_filter(f: str):
    """Tiny SCIM filter parser. Handles `<field> eq "<value>"`."""
    if not f:
        return None
    f = f.strip()
    # Find ' eq '
    parts = f.split(" eq ", 1)
    if len(parts) != 2:
        return None
    field = parts[0].strip()
    value = parts[1].strip()
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    elif value.startswith("'") and value.endswith("'"):
        value = value[1:-1]
    return (field, value)


def get_user(user_id: str) -> dict:
    from . import rbac as _rbac
    user = _rbac._USERS.get(user_id)
    if not user:
        return scim_error(404, f"User {user_id} not found")
    return to_scim(user)


def create_user(payload: dict) -> dict:
    """
    POST /scim/v2/Users.

    Steps:
      1. Validate userName (= email) exists.
      2. If a user with this email already exists, return 409 (Okta will then
         GET to see what we have).
      3. Create new rbac user record, slug user_id from email local-part.
      4. Apply onboarding template if `title` (= job_role) is provided.
      5. Return 201 with the SCIM resource.
    """
    from . import rbac as _rbac

    flat = _flatten_scim_payload(payload)
    email = flat.get("email")
    if not email or "@" not in email:
        return scim_error(400, "userName (email) is required")

    # Conflict check
    for u in _rbac._USERS.values():
        if (u.get("email") or "").lower() == email:
            return scim_error(409, f"User with userName {email} already exists",
                              scim_type="uniqueness")

    # Slug user_id from local part
    base = email.split("@")[0]
    user_id = base
    n = 1
    while user_id in _rbac._USERS:
        n += 1
        user_id = f"{base}-{n}"

    now = datetime.utcnow().isoformat()
    role = flat.get("role") or "learner"

    _rbac._USERS[user_id] = {
        "user_id": user_id,
        "email": email,
        "name": flat.get("name") or base.replace(".", " ").title(),
        "role": role,
        "department": flat.get("department", ""),
        "manager_user_id": _resolve_manager(flat.get("manager_ref")),
        "is_active": flat.get("active", True),
        "job_role": flat.get("job_role"),
        "created_at": now,
        "last_active_at": now,
        "scim_external_id": flat.get("scim_external_id"),
    }
    _log_sync("create", user_id)

    # Trigger onboarding if job_role provided
    if flat.get("job_role"):
        try:
            from . import onboarding as _onboarding
            _onboarding.apply_onboarding(user_id, flat["job_role"])
        except Exception as exc:
            print(f"[scim] onboarding apply on create failed: {exc}")

    return to_scim(_rbac._USERS[user_id])


def replace_user(user_id: str, payload: dict) -> dict:
    """PUT /scim/v2/Users/<id> — full replacement of mutable fields."""
    from . import rbac as _rbac
    user = _rbac._USERS.get(user_id)
    if not user:
        return scim_error(404, f"User {user_id} not found")

    flat = _flatten_scim_payload(payload)
    if flat.get("email"):
        user["email"] = flat["email"]
    if flat.get("name"):
        user["name"] = flat["name"]
    if "active" in flat:
        user["is_active"] = flat["active"]
    if flat.get("department") is not None:
        user["department"] = flat["department"]
    if flat.get("manager_ref"):
        resolved = _resolve_manager(flat["manager_ref"])
        if resolved:
            user["manager_user_id"] = resolved
    if flat.get("scim_external_id"):
        user["scim_external_id"] = flat["scim_external_id"]
    if flat.get("job_role"):
        user["job_role"] = flat["job_role"]
    if flat.get("role"):
        user["role"] = flat["role"]
    user["updated_at"] = datetime.utcnow().isoformat()
    _log_sync("replace", user_id)
    return to_scim(user)


def patch_user(user_id: str, payload: dict) -> dict:
    """
    PATCH /scim/v2/Users/<id> — Okta's go-to for activate/deactivate.

    Okta sends:
      { "schemas": ["urn:...:PatchOp"],
        "Operations": [{"op": "replace", "value": {"active": false}}] }

    Or with paths:
      { "Operations": [{"op": "replace", "path": "active", "value": false}] }
    """
    from . import rbac as _rbac
    user = _rbac._USERS.get(user_id)
    if not user:
        return scim_error(404, f"User {user_id} not found")

    ops = payload.get("Operations") or []
    if not ops:
        return scim_error(400, "PatchOp must include Operations")

    for op in ops:
        action = (op.get("op") or "").lower()
        path = op.get("path")
        value = op.get("value")
        if action not in ("add", "replace", "remove"):
            continue

        # Path-less op: value is a dict of fields
        if not path and isinstance(value, dict):
            if "active" in value:
                user["is_active"] = bool(value["active"])
            if "userName" in value:
                user["email"] = (value["userName"] or "").lower()
            if "displayName" in value:
                user["name"] = value["displayName"]
            if "title" in value:
                user["job_role"] = (value["title"] or "").lower() or None
            if "name" in value and isinstance(value["name"], dict):
                if value["name"].get("formatted"):
                    user["name"] = value["name"]["formatted"]
            ent = value.get(SCIM_ENTERPRISE_EXT)
            if isinstance(ent, dict):
                if "department" in ent:
                    user["department"] = ent.get("department") or ""
                mgr = ent.get("manager")
                if isinstance(mgr, dict):
                    resolved = _resolve_manager(mgr.get("value"))
                    if resolved:
                        user["manager_user_id"] = resolved
            continue

        # Path-targeted op: path is e.g. "active" or "name.formatted"
        if path:
            key = path.lower()
            if key == "active":
                user["is_active"] = bool(value) if action != "remove" else False
            elif key == "username":
                user["email"] = (value or "").lower() if action != "remove" else user["email"]
            elif key in ("displayname", "name.formatted"):
                user["name"] = value if action != "remove" else user["name"]
            elif key == "title":
                user["job_role"] = (value or "").lower() if action != "remove" else None
            elif key == f"{SCIM_ENTERPRISE_EXT}:department" or key == "department":
                user["department"] = value if action != "remove" else ""
            elif "manager" in key:
                if isinstance(value, dict):
                    resolved = _resolve_manager(value.get("value"))
                    if resolved:
                        user["manager_user_id"] = resolved
                elif isinstance(value, str):
                    resolved = _resolve_manager(value)
                    if resolved:
                        user["manager_user_id"] = resolved

    user["updated_at"] = datetime.utcnow().isoformat()
    _log_sync("patch", user_id)
    return to_scim(user)


def delete_user(user_id: str) -> dict:
    """
    DELETE /scim/v2/Users/<id>.

    Per spec, this is "hard delete" — but we soft-delete (is_active=false)
    so audit + Resume + Gigs entries keep their identity attached. The user
    can be reactivated via PATCH active=true.
    """
    from . import rbac as _rbac
    user = _rbac._USERS.get(user_id)
    if not user:
        return scim_error(404, f"User {user_id} not found")
    user["is_active"] = False
    user["updated_at"] = datetime.utcnow().isoformat()
    _log_sync("delete", user_id)
    return {"ok": True, "soft_deleted": True}


# ──────────────────────────────────────────────────────────────
# Discovery endpoints (Okta calls these on connection setup)
# ──────────────────────────────────────────────────────────────

def service_provider_config() -> dict:
    return {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
        "documentationUri": "https://aasan.example.com/docs/scim",
        "patch":         {"supported": True},
        "bulk":          {"supported": False, "maxOperations": 0, "maxPayloadSize": 0},
        "filter":        {"supported": True, "maxResults": 200},
        "changePassword":{"supported": False},
        "sort":          {"supported": False},
        "etag":          {"supported": False},
        "authenticationSchemes": [
            {
                "name": "OAuth Bearer Token",
                "description": "Authentication via OAuth2 bearer token (Aasan-issued).",
                "specUri": "https://www.rfc-editor.org/info/rfc6750",
                "type": "oauthbearertoken",
                "primary": True,
            }
        ],
        "meta": {"resourceType": "ServiceProviderConfig", "location": "/scim/v2/ServiceProviderConfig"},
    }


def resource_types() -> dict:
    return {
        "schemas": [SCIM_LIST_SCHEMA],
        "totalResults": 1,
        "Resources": [
            {
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
                "id": "User",
                "name": "User",
                "endpoint": "/Users",
                "description": "Aasan platform user",
                "schema": SCIM_USER_SCHEMA,
                "schemaExtensions": [
                    {"schema": SCIM_ENTERPRISE_EXT, "required": False}
                ],
                "meta": {"resourceType": "ResourceType", "location": "/scim/v2/ResourceTypes/User"},
            }
        ],
    }


def schemas() -> dict:
    user_schema = {
        "id": SCIM_USER_SCHEMA,
        "name": "User",
        "description": "Aasan SCIM User schema (subset of SCIM core).",
        "attributes": [
            {"name": "userName", "type": "string", "required": True, "uniqueness": "server"},
            {"name": "name", "type": "complex", "subAttributes": [
                {"name": "formatted", "type": "string"},
                {"name": "givenName", "type": "string"},
                {"name": "familyName", "type": "string"},
            ]},
            {"name": "displayName", "type": "string"},
            {"name": "title", "type": "string", "description": "Job role for onboarding template mapping (e.g. software_engineer)"},
            {"name": "userType", "type": "string", "description": "Aasan role (learner, manager, ld_admin, etc.)"},
            {"name": "active", "type": "boolean"},
            {"name": "emails", "type": "complex", "multiValued": True},
            {"name": "externalId", "type": "string"},
        ],
    }
    ent_schema = {
        "id": SCIM_ENTERPRISE_EXT,
        "name": "EnterpriseUser",
        "description": "SCIM Enterprise User extension (department, manager).",
        "attributes": [
            {"name": "department", "type": "string"},
            {"name": "manager", "type": "complex", "subAttributes": [
                {"name": "value", "type": "string"},
                {"name": "displayName", "type": "string"},
            ]},
        ],
    }
    return {
        "schemas": [SCIM_LIST_SCHEMA],
        "totalResults": 2,
        "Resources": [user_schema, ent_schema],
    }


def scim_error(status: int, detail: str, scim_type: str = "") -> dict:
    body = {
        "schemas": [SCIM_ERROR_SCHEMA],
        "status": str(status),
        "detail": detail,
    }
    if scim_type:
        body["scimType"] = scim_type
    body["_status_code"] = status  # consumed by the Flask view
    return body
