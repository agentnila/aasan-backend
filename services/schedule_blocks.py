"""
Schedule blocks — path step → calendar event linkage (Tier 0 follow-on).

Dual-mode persistence: writes/reads to/from Supabase Postgres
`schedule_blocks` table when env vars are configured. Falls back to
in-memory list (`_FALLBACK`) when Postgres is unavailable so local dev
and demo mode keep working.

The block dict shape mirrors V2 Data Model Section 2 Table 18 — every
field the existing /calendar/* routes already use. Postgres is the
source of truth when enabled; the fallback list is process-local.

Public surface:
  add(block)                      → block (with block_id assigned)
  list_for_user(user_id, ...)     → list[dict]
  find(block_id)                  → dict | None
  find_for_step(user_id, ...)     → dict | None  (single active block per step)
  update(block_id, **fields)      → dict | None
  count_active(user_id)           → int
  due_nudges(now)                 → list[dict]   (status=scheduled, nudge_at <= now, nudge_sent_at IS NULL)

The existing app.py SCHEDULE_BLOCKS list keeps working as the in-memory
fallback — `_FALLBACK` aliases it via the `wrap_existing_list()` helper
during module import so we don't need to migrate every call site at
once. Reads on the fallback list still see new entries that landed via
add().
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from . import db

logger = logging.getLogger(__name__)


# In-memory fallback. The app.py SCHEDULE_BLOCKS list aliases to this
# at import time via wrap_existing_list() so every existing read path
# (filter SCHEDULE_BLOCKS by user_id, etc.) still works — and add()
# appends here so they see the new row too. When db.is_enabled() is
# True, Postgres is the source of truth on subsequent reads, but we
# still append to keep the in-process cache hot.
_FALLBACK: list[dict] = []
_ID_COUNTER = [0]


def wrap_existing_list(existing: list) -> list:
    """
    Called from app.py to alias SCHEDULE_BLOCKS to our _FALLBACK list.
    After this call, mutations to either reference appear in both —
    they're the same object. This lets us migrate progressively without
    touching every grep'able SCHEDULE_BLOCKS reference site at once.
    """
    global _FALLBACK
    _FALLBACK = existing
    return _FALLBACK


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _next_id() -> int:
    """Next block_id when Postgres isn't enabled (fallback mode)."""
    _ID_COUNTER[0] += 1
    # If existing entries have higher IDs (e.g. legacy in-memory state),
    # bump past them so we never collide.
    if _FALLBACK:
        max_existing = max((int(b.get("block_id") or 0) for b in _FALLBACK), default=0)
        if max_existing >= _ID_COUNTER[0]:
            _ID_COUNTER[0] = max_existing + 1
    return _ID_COUNTER[0]


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────

def add(block: dict) -> dict:
    """
    Persist a new block. The block dict comes from /calendar/book in
    app.py with all the existing fields populated — we just hand it
    off to the storage layer.

    Always also appends to the in-memory list so the existing app.py
    code that filters/iterates SCHEDULE_BLOCKS keeps working without
    a Postgres round-trip per access.

    Returns the block (with block_id potentially overridden by the
    Postgres bigserial when enabled).
    """
    if db.is_enabled():
        try:
            row = db.execute_returning(
                """
                INSERT INTO schedule_blocks
                    (user_id, goal_id, path_step_id, step_title,
                     start_at, end_at, calendar_event_id, calendar_event_url,
                     status, nudge_at, original_start_at, description, mode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING block_id, user_id, goal_id, path_step_id, step_title,
                          start_at, end_at, duration_minutes,
                          calendar_event_id, calendar_event_url, status,
                          nudge_at, nudge_sent_at, reschedule_count,
                          original_start_at, description, mode,
                          created_at, updated_at
                """,
                (
                    block.get("employee_id") or block.get("user_id"),
                    block.get("goal_id"),
                    block.get("path_step_id"),
                    block.get("step_title", ""),
                    block.get("start_at"),
                    block.get("end_at"),
                    block.get("calendar_event_id"),
                    block.get("calendar_event_url"),
                    block.get("status", "scheduled"),
                    block.get("nudge_at"),
                    block.get("original_start_at") or block.get("start_at"),
                    block.get("description"),
                    block.get("mode"),
                ),
            )
            if row:
                normalized = _normalize(row)
                # Replace the block_id with the bigserial Postgres assigned
                block["block_id"] = normalized["block_id"]
                # Ensure the legacy in-memory list also sees this row
                _FALLBACK.append(block)
                return normalized
        except Exception as exc:
            logger.warning("schedule_blocks insert to Postgres failed (%s) — falling back to in-memory", exc)

    # Fallback: assign id locally + append
    if "block_id" not in block or block["block_id"] is None:
        block["block_id"] = _next_id()
    if "created_at" not in block:
        block["created_at"] = _now_iso()
    _FALLBACK.append(block)
    return block


def list_for_user(user_id: str, include_past: bool = False, limit: int = 100) -> list[dict]:
    """
    Return the user's blocks, sorted by start_at ascending. Past blocks
    (end_at < now) are filtered unless include_past is True.

    Works against Postgres when enabled; falls back to filtering
    _FALLBACK otherwise.
    """
    if db.is_enabled():
        clauses = ["user_id = %s"]
        params: list[Any] = [user_id]
        if not include_past:
            clauses.append("end_at >= now() - interval '1 hour'")  # tolerance for in-flight blocks
            clauses.append("status NOT IN ('cancelled')")
        try:
            rows = db.query(
                f"""
                SELECT block_id, user_id, goal_id, path_step_id, step_title,
                       start_at, end_at, duration_minutes,
                       calendar_event_id, calendar_event_url, status,
                       nudge_at, nudge_sent_at, reschedule_count,
                       original_start_at, description, mode,
                       created_at, updated_at
                FROM schedule_blocks
                WHERE {' AND '.join(clauses)}
                ORDER BY start_at ASC
                LIMIT %s
                """,
                params + [limit],
            )
            if rows is not None:
                return [_normalize(r) for r in rows]
        except Exception as exc:
            logger.warning("schedule_blocks list from Postgres failed (%s) — using fallback", exc)

    items = [b for b in _FALLBACK if (b.get("employee_id") or b.get("user_id")) == user_id]
    if not include_past:
        now_iso = _now_iso()
        items = [b for b in items if (b.get("end_at") or "") >= now_iso[:13]]  # rough hour-grain filter
        items = [b for b in items if b.get("status") != "cancelled"]
    items = sorted(items, key=lambda b: b.get("start_at") or "")
    return items[:limit]


def find(block_id: int) -> dict | None:
    """Lookup a single block by id. Returns None when not found."""
    try:
        block_id_int = int(block_id)
    except (TypeError, ValueError):
        return None

    if db.is_enabled():
        try:
            row = db.query_one(
                """
                SELECT block_id, user_id, goal_id, path_step_id, step_title,
                       start_at, end_at, duration_minutes,
                       calendar_event_id, calendar_event_url, status,
                       nudge_at, nudge_sent_at, reschedule_count,
                       original_start_at, description, mode,
                       created_at, updated_at
                FROM schedule_blocks WHERE block_id = %s
                """,
                (block_id_int,),
            )
            if row:
                return _normalize(row)
        except Exception as exc:
            logger.warning("schedule_blocks find from Postgres failed (%s) — using fallback", exc)

    return next((b for b in _FALLBACK if int(b.get("block_id") or 0) == block_id_int), None)


def find_for_step(user_id: str, goal_id: str | None, path_step_id: str) -> dict | None:
    """
    Find an active (scheduled / rescheduled / in_progress) block for a
    specific path step. Returns the most recent one when there are
    duplicates. Used by the frontend to render "📅 Scheduled: ..." inline
    on the step.
    """
    if db.is_enabled():
        try:
            row = db.query_one(
                """
                SELECT block_id, user_id, goal_id, path_step_id, step_title,
                       start_at, end_at, duration_minutes,
                       calendar_event_id, calendar_event_url, status,
                       nudge_at, nudge_sent_at, reschedule_count,
                       original_start_at, description, mode,
                       created_at, updated_at
                FROM schedule_blocks
                WHERE user_id = %s
                  AND path_step_id = %s
                  AND status IN ('scheduled', 'rescheduled', 'in_progress')
                ORDER BY start_at DESC
                LIMIT 1
                """,
                (user_id, path_step_id),
            )
            if row:
                return _normalize(row)
        except Exception as exc:
            logger.warning("schedule_blocks find_for_step from Postgres failed (%s) — using fallback", exc)

    candidates = [
        b for b in _FALLBACK
        if (b.get("employee_id") or b.get("user_id")) == user_id
        and b.get("path_step_id") == path_step_id
        and b.get("status") in ("scheduled", "rescheduled", "in_progress")
    ]
    if not candidates:
        return None
    return sorted(candidates, key=lambda b: b.get("start_at") or "", reverse=True)[0]


def update(block_id: int, **fields) -> dict | None:
    """Apply field updates. Returns the updated block or None."""
    try:
        block_id_int = int(block_id)
    except (TypeError, ValueError):
        return None

    if db.is_enabled():
        sets: list[str] = []
        params: list[Any] = []
        for key in ("status", "start_at", "end_at", "calendar_event_id",
                    "calendar_event_url", "nudge_at", "nudge_sent_at",
                    "reschedule_count", "description"):
            if key in fields:
                sets.append(f"{key} = %s")
                params.append(fields[key])
        if not sets:
            return find(block_id_int)
        params.append(block_id_int)
        try:
            row = db.execute_returning(
                f"""
                UPDATE schedule_blocks SET {', '.join(sets)}
                WHERE block_id = %s
                RETURNING block_id, user_id, goal_id, path_step_id, step_title,
                          start_at, end_at, duration_minutes,
                          calendar_event_id, calendar_event_url, status,
                          nudge_at, nudge_sent_at, reschedule_count,
                          original_start_at, description, mode,
                          created_at, updated_at
                """,
                params,
            )
            if row:
                normalized = _normalize(row)
                # Mirror to fallback list
                for b in _FALLBACK:
                    if int(b.get("block_id") or 0) == block_id_int:
                        b.update({k: v for k, v in fields.items()})
                        break
                return normalized
        except Exception as exc:
            logger.warning("schedule_blocks update Postgres failed (%s) — using fallback", exc)

    block = next((b for b in _FALLBACK if int(b.get("block_id") or 0) == block_id_int), None)
    if not block:
        return None
    for k, v in fields.items():
        block[k] = v
    return block


def count_active(user_id: str) -> int:
    """How many scheduled/rescheduled/in_progress blocks the user has."""
    if db.is_enabled():
        try:
            row = db.query_one(
                """
                SELECT COUNT(*) AS n
                FROM schedule_blocks
                WHERE user_id = %s
                  AND status IN ('scheduled', 'rescheduled', 'in_progress')
                """,
                (user_id,),
            )
            if row:
                return int(row["n"])
        except Exception as exc:
            logger.warning("schedule_blocks count_active failed (%s) — using fallback", exc)

    return sum(
        1 for b in _FALLBACK
        if (b.get("employee_id") or b.get("user_id")) == user_id
        and b.get("status") in ("scheduled", "rescheduled", "in_progress")
    )


def due_nudges(now_iso: str | None = None) -> list[dict]:
    """
    Blocks whose 5-min nudge window has arrived but hasn't fired yet.
    Used by /cron/calendar_nudges.
    """
    cutoff = now_iso or _now_iso()
    if db.is_enabled():
        try:
            rows = db.query(
                """
                SELECT block_id, user_id, goal_id, path_step_id, step_title,
                       start_at, end_at, duration_minutes,
                       calendar_event_id, calendar_event_url, status,
                       nudge_at, nudge_sent_at, reschedule_count,
                       original_start_at, description, mode
                FROM schedule_blocks
                WHERE status = 'scheduled'
                  AND nudge_sent_at IS NULL
                  AND nudge_at <= %s
                ORDER BY nudge_at ASC
                """,
                (cutoff,),
            )
            if rows is not None:
                return [_normalize(r) for r in rows]
        except Exception as exc:
            logger.warning("schedule_blocks due_nudges failed (%s) — using fallback", exc)

    return [
        b for b in _FALLBACK
        if b.get("status") == "scheduled"
        and not b.get("nudge_sent_at")
        and (b.get("nudge_at") or "") <= cutoff
    ]


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _normalize(row: dict) -> dict:
    """Normalize a Postgres row to the legacy block-dict shape."""
    out = dict(row)
    # Legacy callers expect 'employee_id' AND 'user_id' both populated
    if "employee_id" not in out and "user_id" in out:
        out["employee_id"] = out["user_id"]
    for k in ("start_at", "end_at", "nudge_at", "nudge_sent_at",
              "original_start_at", "created_at", "updated_at"):
        v = out.get(k)
        if v is not None and hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    return out
