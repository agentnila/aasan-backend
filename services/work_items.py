"""
Work_Items — granular build-task tracker (V3).

Companion to BUILD_LOG.md. BUILD_LOG.md is ~10 entries/quarter (one per ship,
narrative). Work_Items is hundreds of granular tasks — what's currently
in_progress, what's blocked, what's pending, with status transitions.

Dual-mode persistence: writes to Supabase Postgres table `work_items` when
SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY env vars are set; otherwise falls
back to an in-memory list (local dev, demo mode). Shape matches Table 27 in
`Aasan_V2_Data_Model.md` and the Postgres DDL in `migrations/0001_init.sql`.

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

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from . import db

logger = logging.getLogger(__name__)

# In-memory fallback store; primary persistence is Supabase Postgres `work_items`
_STORE: list[dict] = []
_ID_COUNTER = [0]

VALID_STATUSES = {"pending", "in_progress", "completed", "blocked", "deleted"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _next_id() -> int:
    _ID_COUNTER[0] += 1
    return _ID_COUNTER[0]


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────

def create(title: str, **fields) -> dict:
    if not title or not title.strip():
        return {"error": "title required"}
    status = fields.get("status", "pending")
    if status not in VALID_STATUSES:
        return {"error": f"status must be one of {sorted(VALID_STATUSES)}"}

    title_clean = title.strip()
    description = fields.get("description", "") or ""
    owner = fields.get("owner") or "balaji"
    parent_ship_date = fields.get("parent_ship_date")
    tags = list(fields.get("tags") or [])
    estimated_minutes = fields.get("estimated_minutes")
    actual_minutes = fields.get("actual_minutes")

    if db.is_enabled():
        try:
            row = db.execute_returning(
                """
                INSERT INTO work_items
                    (title, status, description, owner, parent_ship_date,
                     tags, estimated_minutes, actual_minutes, completed_at)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s,
                     CASE WHEN %s = 'completed' THEN now() ELSE NULL END)
                RETURNING work_item_id, title, status, description, owner,
                          parent_ship_date, tags, estimated_minutes,
                          actual_minutes, created_at, updated_at, completed_at
                """,
                (
                    title_clean, status, description, owner, parent_ship_date,
                    tags, estimated_minutes, actual_minutes, status,
                ),
            )
            if row:
                return _normalize(row)
        except Exception as exc:
            logger.warning("work_items.create Postgres failed (%s) — using fallback", exc)

    now = _now_iso()
    item = {
        "work_item_id": _next_id(),
        "title": title_clean,
        "status": status,
        "description": description,
        "owner": owner,
        "parent_ship_date": parent_ship_date,
        "tags": tags,
        "estimated_minutes": estimated_minutes,
        "actual_minutes": actual_minutes,
        "created_at": now,
        "updated_at": now,
        "completed_at": now if status == "completed" else None,
    }
    _STORE.append(item)
    return item


def update(item_id: int, **fields) -> dict:
    try:
        item_id_int = int(item_id)
    except (TypeError, ValueError):
        return {"error": f"work_item_id must be integer, got {item_id!r}"}

    if "status" in fields and fields["status"] not in VALID_STATUSES:
        return {"error": f"status must be one of {sorted(VALID_STATUSES)}"}

    if db.is_enabled():
        sets: list[str] = []
        params: list[Any] = []

        if "status" in fields:
            sets.append("status = %s")
            params.append(fields["status"])
            # Set/clear completed_at on transitions
            if fields["status"] == "completed":
                sets.append("completed_at = COALESCE(completed_at, now())")
            else:
                sets.append("completed_at = NULL")

        for key in ("title", "description", "owner", "parent_ship_date",
                    "estimated_minutes", "actual_minutes"):
            if key in fields:
                sets.append(f"{key} = %s")
                params.append(fields[key])

        if "tags" in fields:
            sets.append("tags = %s")
            params.append(list(fields["tags"] or []))

        if not sets:
            # No-op update — just fetch current row
            return get(item_id_int) or {"error": f"work_item {item_id_int} not found"}

        params.append(item_id_int)
        try:
            row = db.execute_returning(
                f"""
                UPDATE work_items SET {', '.join(sets)}
                WHERE work_item_id = %s
                RETURNING work_item_id, title, status, description, owner,
                          parent_ship_date, tags, estimated_minutes,
                          actual_minutes, created_at, updated_at, completed_at
                """,
                params,
            )
            if row:
                return _normalize(row)
            return {"error": f"work_item {item_id_int} not found"}
        except Exception as exc:
            logger.warning("work_items.update Postgres failed (%s) — using fallback", exc)

    # Fallback path
    item = _find_in_memory(item_id_int)
    if not item:
        return {"error": f"work_item {item_id_int} not found"}

    if "status" in fields:
        new_status = fields["status"]
        prev = item["status"]
        item["status"] = new_status
        if new_status == "completed" and prev != "completed":
            item["completed_at"] = _now_iso()
        elif new_status != "completed":
            item["completed_at"] = None

    for k in ("title", "description", "owner", "parent_ship_date", "tags",
              "estimated_minutes", "actual_minutes"):
        if k in fields:
            item[k] = fields[k]

    item["updated_at"] = _now_iso()
    return item


def get(item_id: int) -> dict:
    try:
        item_id_int = int(item_id)
    except (TypeError, ValueError):
        return {"error": f"work_item_id must be integer, got {item_id!r}"}

    if db.is_enabled():
        try:
            row = db.query_one(
                """
                SELECT work_item_id, title, status, description, owner,
                       parent_ship_date, tags, estimated_minutes,
                       actual_minutes, created_at, updated_at, completed_at
                FROM work_items WHERE work_item_id = %s
                """,
                (item_id_int,),
            )
            if row:
                return _normalize(row)
            return {"error": f"work_item {item_id_int} not found"}
        except Exception as exc:
            logger.warning("work_items.get Postgres failed (%s) — using fallback", exc)

    item = _find_in_memory(item_id_int)
    return item or {"error": f"work_item {item_id_int} not found"}


def list_items(
    status: str | None = None,
    tag: str | None = None,
    owner: str | None = None,
    parent_ship_date: str | None = None,
    limit: int = 100,
    include_deleted: bool = False,
) -> dict:
    limit_int = max(0, int(limit or 0))

    if db.is_enabled():
        clauses: list[str] = []
        params: list[Any] = []
        if not include_deleted:
            clauses.append("status != 'deleted'")
        if status:
            clauses.append("status = %s")
            params.append(status)
        if tag:
            needle = tag.lstrip("#").lower()
            clauses.append("EXISTS (SELECT 1 FROM unnest(tags) t WHERE LOWER(LTRIM(t, '#')) = %s)")
            params.append(needle)
        if owner:
            clauses.append("owner = %s")
            params.append(owner)
        if parent_ship_date:
            clauses.append("parent_ship_date = %s")
            params.append(parent_ship_date)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order = """
            ORDER BY
                CASE status
                    WHEN 'in_progress' THEN 0
                    WHEN 'pending'     THEN 1
                    WHEN 'blocked'     THEN 2
                    WHEN 'completed'   THEN 3
                    WHEN 'deleted'     THEN 4
                    ELSE 9
                END,
                work_item_id DESC
        """
        if limit_int > 0:
            params.append(limit_int)
            limit_clause = "LIMIT %s"
        else:
            limit_clause = ""

        try:
            rows = db.query(
                f"""
                SELECT work_item_id, title, status, description, owner,
                       parent_ship_date, tags, estimated_minutes,
                       actual_minutes, created_at, updated_at, completed_at
                FROM work_items
                {where}
                {order}
                {limit_clause}
                """,
                params,
            )
            if rows is not None:
                items = [_normalize(r) for r in rows]
                counts_rows = db.query(
                    "SELECT status, COUNT(*) AS n FROM work_items GROUP BY status"
                ) or []
                counts = {s: 0 for s in VALID_STATUSES}
                for r in counts_rows:
                    counts[r["status"]] = int(r["n"])
                return {"items": items, "count": len(items), "totals_by_status": counts}
        except Exception as exc:
            logger.warning("work_items.list Postgres failed (%s) — using fallback", exc)

    # Fallback path
    items = _STORE
    if not include_deleted:
        items = [i for i in items if i["status"] != "deleted"]
    if status:
        items = [i for i in items if i["status"] == status]
    if tag:
        needle = tag.lstrip("#").lower()
        items = [i for i in items if needle in [t.lstrip("#").lower() for t in (i.get("tags") or [])]]
    if owner:
        items = [i for i in items if i.get("owner") == owner]
    if parent_ship_date:
        items = [i for i in items if i.get("parent_ship_date") == parent_ship_date]

    status_rank = {"in_progress": 0, "pending": 1, "blocked": 2, "completed": 3, "deleted": 4}
    items = sorted(items, key=lambda i: (status_rank.get(i["status"], 9), -int(i["work_item_id"])))
    if limit_int > 0:
        items = items[:limit_int]

    counts = {s: 0 for s in VALID_STATUSES}
    for i in _STORE:
        counts[i["status"]] = counts.get(i["status"], 0) + 1
    return {"items": items, "count": len(items), "totals_by_status": counts}


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _find_in_memory(item_id: int) -> dict | None:
    return next((i for i in _STORE if i["work_item_id"] == item_id), None)


def _normalize(row: dict) -> dict:
    """Normalize a Postgres row to JSON-serializable dict."""
    out = dict(row)
    for ts_key in ("created_at", "updated_at", "completed_at"):
        v = out.get(ts_key)
        if isinstance(v, datetime):
            out[ts_key] = v.isoformat()
    psd = out.get("parent_ship_date")
    if hasattr(psd, "isoformat") and not isinstance(psd, str):
        out["parent_ship_date"] = psd.isoformat()
    out["tags"] = list(out.get("tags") or [])
    return out
