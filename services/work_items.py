"""
Work_Items — granular build-task tracker (V3).

Companion to JOURNAL.md. JOURNAL.md is ~10 entries/quarter (one per ship,
narrative). Work_Items is hundreds of granular tasks — what's currently
in_progress, what's blocked, what's pending, with status transitions.

Same dual-purpose persistence pattern as the rest of the codebase: lives
in-memory now (Phase 1); flips to Airtable Table 26 in the codebase-wide
migration (Phase 2). Shape mirrors Table 26 exactly so migration is a
straight copy.

INTERFACE
─────────
  create(title, **fields) -> item dict
  update(item_id, **fields) -> item dict
  get(item_id) -> item dict | None
  list(status?, tag?, owner?, parent_ship_date?, limit?) -> list of items

STATUS WORKFLOW
───────────────
  pending → in_progress → completed
  any → blocked → (back to in_progress)
  any → deleted (soft delete; filtered out of list by default)
"""

from datetime import datetime

# Phase 1 in-memory store; Phase 2 → Airtable Table 26
_STORE = []
_ID_COUNTER = [0]

VALID_STATUSES = {"pending", "in_progress", "completed", "blocked", "deleted"}


def _now():
    return datetime.utcnow().isoformat()


def _next_id():
    _ID_COUNTER[0] += 1
    return _ID_COUNTER[0]


def create(title: str, **fields) -> dict:
    if not title or not title.strip():
        return {"error": "title required"}
    status = fields.get("status", "pending")
    if status not in VALID_STATUSES:
        return {"error": f"status must be one of {sorted(VALID_STATUSES)}"}
    now = _now()
    item = {
        "work_item_id": _next_id(),
        "title": title.strip(),
        "status": status,
        "description": fields.get("description", ""),
        "owner": fields.get("owner", "balaji"),
        "parent_ship_date": fields.get("parent_ship_date"),  # links to a JOURNAL.md ## YYYY-MM-DD entry
        "tags": list(fields.get("tags") or []),
        "estimated_minutes": fields.get("estimated_minutes"),
        "actual_minutes": fields.get("actual_minutes"),
        "created_at": now,
        "updated_at": now,
        "completed_at": now if status == "completed" else None,
    }
    _STORE.append(item)
    return item


def update(item_id: int, **fields) -> dict:
    item = _find(item_id)
    if not item:
        return {"error": f"work_item {item_id} not found"}

    if "status" in fields:
        new_status = fields["status"]
        if new_status not in VALID_STATUSES:
            return {"error": f"status must be one of {sorted(VALID_STATUSES)}"}
        prev = item["status"]
        item["status"] = new_status
        if new_status == "completed" and prev != "completed":
            item["completed_at"] = _now()
        elif new_status != "completed":
            item["completed_at"] = None

    for k in ("title", "description", "owner", "parent_ship_date", "tags", "estimated_minutes", "actual_minutes"):
        if k in fields:
            item[k] = fields[k]

    item["updated_at"] = _now()
    return item


def get(item_id: int) -> dict:
    item = _find(item_id)
    return item or {"error": f"work_item {item_id} not found"}


def list_items(status: str = None, tag: str = None, owner: str = None,
               parent_ship_date: str = None, limit: int = 100,
               include_deleted: bool = False) -> dict:
    items = _STORE
    if not include_deleted:
        items = [i for i in items if i["status"] != "deleted"]
    if status:
        items = [i for i in items if i["status"] == status]
    if tag:
        needle = tag.lstrip("#").lower()
        items = [i for i in items if needle in [t.lower() for t in (i.get("tags") or [])]]
    if owner:
        items = [i for i in items if i.get("owner") == owner]
    if parent_ship_date:
        items = [i for i in items if i.get("parent_ship_date") == parent_ship_date]

    # Sort: in_progress first, then pending, then blocked, completed last; newest within group
    status_rank = {"in_progress": 0, "pending": 1, "blocked": 2, "completed": 3, "deleted": 4}
    items = sorted(items, key=lambda i: (status_rank.get(i["status"], 9), -int(i["work_item_id"])))
    items = items[:limit] if limit and limit > 0 else items

    counts = {s: 0 for s in VALID_STATUSES}
    for i in _STORE:
        counts[i["status"]] = counts.get(i["status"], 0) + 1
    return {"items": items, "count": len(items), "totals_by_status": counts}


def _find(item_id):
    try:
        target = int(item_id)
    except (TypeError, ValueError):
        return None
    return next((i for i in _STORE if i["work_item_id"] == target), None)
