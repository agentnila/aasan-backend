"""
Path Adjustment Engine — V3.

Goals each have a persistent live learning path. The engine adjusts the path
in response to:
  - session_complete   (mastery captured / gap detected)
  - content_added      (new content fits the path)
  - staleness_flag     (Currency Watch flagged a step's source as stale)
  - assignment_create  (manager assigned content into the path)
  - learner_edit       (manual reorder / insert / skip)

Each adjustment writes a diff back to the path AND a one-line entry to
recompute_history (visible to the learner — full transparency).

STORAGE
───────
Dual-mode: persists to Supabase Postgres tables `goals`, `paths`, `path_steps`,
`path_recomputes` when SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY env vars are set.
Falls back to an in-memory dict keyed by user_id when env vars are absent
(local dev, demo mode). The data shape defined here matches the Postgres
schema in `migrations/0001_init.sql`.

CLAUDE MODE
───────────
When ANTHROPIC_API_KEY is set, the engine prompts Claude Sonnet over
(current_path + employee_state + trigger) to produce a structured diff.
When unset, returns deterministic stub diffs that demonstrate the loop.
"""

import json
import logging
from datetime import datetime
from . import claude_client, db
from . import content_index as content_catalog
from . import resume as _resume_svc
from . import work_items as _work_items_svc

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Demo seed — three goals per demo user, with persistent paths
# ──────────────────────────────────────────────────────────────

def _seed_paths():
    """Initial demo state. Walked at first access; mutations persist in _STORE."""
    return {
        "cloud-architect": {
            "goal": {
                "id": "cloud-architect",
                "name": "Become a Cloud Architect",
                "priority": "primary",
                "objective": "Lead our team's cloud migration, get promoted to Staff Engineer",
                "timeline": "Q4 2026",
                "days_left": 192,
                "success_criteria": "Pass AWS Solutions Architect Pro + lead one migration independently",
                "readiness": 48,
                "delta": "+10 this wk",
                "status": "active",
            },
            "path": {
                "id": "path-cloud-architect",
                "title": "Path to Cloud Architect",
                "progress_pct": 46,
                "current_step_id": "step-6",
                "estimated_total_minutes": 480,
                "last_recompute_reason": "Inserted topology refresher — K8s 1.31 deprecation (Currency Watch)",
                "last_recomputed_at": "2026-04-27T18:00:00Z",
                "recompute_history": [
                    {"date": "2026-04-27", "reason": "Currency Watch — K8s 1.31 topology deprecation", "added": ["topology refresher (3 min)"]},
                    {"date": "2026-04-22", "reason": "Gap detection — IAM weak across path", "added": ["Cloud Security Foundations (90 min)"]},
                ],
                "steps": [
                    {"id": "step-1", "order": 1, "title": "Linux Fundamentals", "step_type": "content", "status": "known", "estimated_minutes": 5, "inserted_by": "engine", "inserted_reason": "auto: skill exists in graph"},
                    {"id": "step-2", "order": 2, "title": "Container Basics", "step_type": "content", "status": "known", "estimated_minutes": 5, "inserted_by": "engine"},
                    {"id": "step-3", "order": 3, "title": "Kubernetes Architecture", "step_type": "content", "status": "done", "estimated_minutes": 45, "actual_minutes": 42, "mastery_at_completion": 0.8, "completed_at": "2026-04-14"},
                    {"id": "step-4", "order": 4, "title": "Pods & Deployments", "step_type": "content", "status": "done", "estimated_minutes": 40, "actual_minutes": 38, "mastery_at_completion": 0.75, "completed_at": "2026-04-18"},
                    {"id": "step-5", "order": 5, "title": "Services & Networking", "step_type": "content", "status": "done", "estimated_minutes": 50, "actual_minutes": 47, "mastery_at_completion": 0.7, "completed_at": "2026-04-22"},
                    {"id": "step-5a", "order": 5.5, "title": "K8s 1.31 topology refresher", "step_type": "refresher", "status": "done", "estimated_minutes": 3, "completed_at": "2026-04-27", "inserted_by": "engine", "inserted_reason": "auto: K8s 1.31 deprecated topologyKeys (Currency Watch)"},
                    {"id": "step-6", "order": 6, "title": "Service Mesh with Istio", "step_type": "content", "status": "active", "estimated_minutes": 30},
                    {"id": "step-7", "order": 7, "title": "AWS Core Services — EC2, S3, VPC", "step_type": "content", "status": "pending", "estimated_minutes": 120},
                    {"id": "step-8", "order": 8, "title": "Infrastructure as Code (Terraform)", "step_type": "content", "status": "pending", "estimated_minutes": 90},
                    {"id": "step-9", "order": 9, "title": "AWS Lambda & Serverless", "step_type": "content", "status": "pending", "estimated_minutes": 60},
                    {"id": "step-10", "order": 10, "title": "Cloud Security Foundations", "step_type": "gap_closure", "status": "pending", "estimated_minutes": 90, "inserted_by": "engine", "inserted_reason": "auto: gap detected — IAM weak across path"},
                    {"id": "step-11", "order": 11, "title": "Migration Patterns", "step_type": "content", "status": "pending", "estimated_minutes": 120},
                    {"id": "step-12", "order": 12, "title": "AWS SA Pro Practice Exam", "step_type": "content", "status": "pending", "estimated_minutes": 240},
                ],
            },
        },
        "compliance": {
            "goal": {
                "id": "compliance",
                "name": "Data Privacy Compliance 2026",
                "priority": "assigned",
                "objective": "Annual mandatory compliance — required by Legal",
                "timeline": "June 30, 2026",
                "days_left": 64,
                "success_criteria": "Complete + retain 80%+ at 30-day spaced review",
                "readiness": 35,
                "delta": "+15 this wk",
                "status": "active",
            },
            "path": {
                "id": "path-compliance",
                "title": "Data Privacy Compliance 2026",
                "progress_pct": 33,
                "current_step_id": "step-c1",
                "estimated_total_minutes": 75,
                "last_recompute_reason": "Pre-marked Data Classification as known (you completed last year's version)",
                "last_recomputed_at": "2026-04-22T10:00:00Z",
                "recompute_history": [
                    {"date": "2026-04-22", "reason": "Engine pre-marked known steps from prior year completion", "added": []},
                ],
                "steps": [
                    {"id": "step-c1", "order": 1, "title": "PII handling for engineers", "step_type": "content", "status": "active", "estimated_minutes": 30},
                    {"id": "step-c2", "order": 2, "title": "Data classification refresher", "step_type": "refresher", "status": "known", "estimated_minutes": 10, "inserted_reason": "auto: prior year completion"},
                    {"id": "step-c3", "order": 3, "title": "Compliance acknowledgment + recall check", "step_type": "review", "status": "pending", "estimated_minutes": 20},
                ],
            },
        },
        "mlops": {
            "goal": {
                "id": "mlops",
                "name": "Learn about MLOps",
                "priority": "exploration",
                "objective": "Curious; might bridge to next role; could combine with Cloud expertise",
                "timeline": "No deadline",
                "days_left": None,
                "success_criteria": "Be able to evaluate 'do we need an MLOps engineer?' decisions confidently",
                "readiness": 12,
                "delta": "new",
                "status": "active",
            },
            "path": {
                "id": "path-mlops",
                "title": "MLOps Exploration",
                "progress_pct": 25,
                "current_step_id": "step-m2",
                "estimated_total_minutes": 600,
                "last_recompute_reason": "Inserted Feature Stores — content matched stated curiosity around data pipelines",
                "last_recomputed_at": "2026-04-23T14:00:00Z",
                "recompute_history": [
                    {"date": "2026-04-23", "reason": "Content-added — Feature Stores course matched stated interest", "added": ["Feature Stores Fundamentals (45 min)"]},
                ],
                "steps": [
                    {"id": "step-m1", "order": 1, "title": "What is MLOps — overview", "step_type": "content", "status": "done", "estimated_minutes": 30, "completed_at": "2026-04-15"},
                    {"id": "step-m2", "order": 2, "title": "Model serving fundamentals", "step_type": "content", "status": "active", "estimated_minutes": 60},
                    {"id": "step-m3", "order": 3, "title": "Feature Stores Fundamentals", "step_type": "content", "status": "pending", "estimated_minutes": 45, "inserted_by": "engine", "inserted_reason": "auto: matched stated curiosity around data pipelines"},
                    {"id": "step-m4", "order": 4, "title": "Model monitoring & drift", "step_type": "content", "status": "pending", "estimated_minutes": 75},
                    {"id": "step-m5", "order": 5, "title": "MLOps stack survey", "step_type": "content", "status": "pending", "estimated_minutes": 90},
                ],
            },
        },
    }


# In-memory fallback store: { user_id: { goal_id: { goal: {...}, path: {...} } } }
# Used when SUPABASE_URL is unset (local dev / demo without DB) and as a
# per-process working buffer between read and write within a single request.
_STORE = {}

# Tracks which users have had demo-user seeded into Postgres this process
_DEMO_SEEDED_USERS_PG: set[str] = set()

# Manager-assigned content waiting to be applied — flushed by recompute.
# Always in-memory (transient queue, not persisted).
_ASSIGNMENT_QUEUE = {}  # { user_id: [{title, source, url, assigned_by, ...}, ...] }


def _ensure_user(user_id: str):
    """
    Return the user's goal/path data as { goal_id: { goal, path } }.

    When db.is_enabled(), reads fresh from Postgres on every call (no inter-
    request caching — the per-request mutation flow is read → mutate → persist).
    The Postgres read populates _STORE so within the same call mutations are
    visible without round-tripping.

    When Postgres isn't configured, uses _STORE as the persistent in-process
    cache. demo-user gets the 3-goal seed on first access.
    """
    if db.is_enabled():
        if user_id == "demo-user" and user_id not in _DEMO_SEEDED_USERS_PG:
            _maybe_seed_demo_user_pg()
            _DEMO_SEEDED_USERS_PG.add(user_id)
        try:
            user_data = _load_user_from_pg(user_id)
            _STORE[user_id] = user_data  # so subsequent _persist_* sees the buffer
            return user_data
        except Exception as exc:
            logger.warning("path_engine PG load failed (%s) — falling back to in-memory", exc)

    if user_id not in _STORE:
        if user_id == "demo-user":
            _STORE[user_id] = _seed_paths()
        else:
            _STORE[user_id] = {}
    return _STORE[user_id]


# ──────────────────────────────────────────────────────────────
# Postgres I/O helpers
# ──────────────────────────────────────────────────────────────

def _maybe_seed_demo_user_pg():
    """If demo-user has no goals in Postgres, write the demo seed."""
    try:
        existing = db.query_one(
            "SELECT COUNT(*) AS n FROM goals WHERE user_id = %s",
            ("demo-user",),
        )
        if existing and int(existing.get("n", 0)) > 0:
            return
        seed = _seed_paths()
        for goal_id, entry in seed.items():
            _STORE.setdefault("demo-user", {})[goal_id] = entry
            _persist_goal_path("demo-user", goal_id)
            for hist_entry in reversed(entry["path"].get("recompute_history") or []):
                _persist_recompute_pg("demo-user", goal_id, hist_entry, trigger="seed")
        logger.info("Seeded demo-user with %d goals into Postgres", len(seed))
    except Exception as exc:
        logger.warning("demo-user PG seed failed (%s)", exc)


def _load_user_from_pg(user_id: str) -> dict:
    """Reconstruct the { goal_id: { goal, path } } shape from Postgres."""
    goal_rows = db.query(
        """
        SELECT goal_id, name, objective, timeline, days_left, success_criteria,
               priority, status, readiness, delta, assigned_by, created_at, updated_at,
               context_source_type, context_url, context_filename, context_mime, context_text
        FROM goals
        WHERE user_id = %s
        ORDER BY created_at
        """,
        (user_id,),
    ) or []
    user_data: dict = {}
    for g in goal_rows:
        goal_id = g["goal_id"]
        path_row = db.query_one(
            """
            SELECT path_id, title, progress_pct, current_step_id,
                   estimated_total_minutes, last_recompute_reason,
                   last_recomputed_at, status, created_at
            FROM paths
            WHERE user_id = %s AND goal_id = %s
            """,
            (user_id, goal_id),
        )
        step_rows = db.query(
            """
            SELECT step_id, step_order, title, step_type, status,
                   estimated_minutes, actual_minutes, mastery_at_completion,
                   inserted_by, inserted_reason, completed_at, inserted_at,
                   content_url, content_provider, content_title, content_id,
                   phase_local_id, step_rationale, is_free
            FROM path_steps
            WHERE user_id = %s AND goal_id = %s
            ORDER BY step_order
            """,
            (user_id, goal_id),
        ) or []
        phase_rows = db.query(
            """
            SELECT phase_local_id, order_index, title, duration_weeks,
                   rationale_md, deliverable_md
            FROM path_phases
            WHERE user_id = %s AND goal_id = %s
            ORDER BY order_index
            """,
            (user_id, goal_id),
        ) or []
        history_rows = db.query(
            """
            SELECT recomputed_at, trigger, reason, diff
            FROM path_recomputes
            WHERE user_id = %s AND goal_id = %s
            ORDER BY recomputed_at DESC
            LIMIT 10
            """,
            (user_id, goal_id),
        ) or []

        user_data[goal_id] = {
            "goal": _row_to_goal(g),
            "path": _rows_to_path(path_row, step_rows, history_rows, goal_id, phase_rows=phase_rows),
        }
    return user_data


def _row_to_goal(row: dict) -> dict:
    return {
        "id": row["goal_id"],
        "name": row.get("name", ""),
        "objective": row.get("objective"),
        "timeline": row.get("timeline"),
        "days_left": row.get("days_left"),
        "success_criteria": row.get("success_criteria"),
        "priority": row.get("priority", "secondary"),
        "status": row.get("status", "active"),
        "readiness": int(row.get("readiness") or 0),
        "delta": row.get("delta"),
        "assigned_by": row.get("assigned_by", "self"),
        "created_at": _iso(row.get("created_at")),
        # Goal context (added in 0004)
        "context_source_type": row.get("context_source_type"),
        "context_url": row.get("context_url"),
        "context_filename": row.get("context_filename"),
        "context_mime": row.get("context_mime"),
        "context_text": row.get("context_text"),
    }


def _rows_to_path(path_row, step_rows, history_rows, goal_id: str, phase_rows=None) -> dict:
    if not path_row:
        return {
            "id": f"path-{goal_id}",
            "title": "",
            "progress_pct": 0,
            "current_step_id": None,
            "estimated_total_minutes": 0,
            "last_recompute_reason": "",
            "last_recomputed_at": None,
            "recompute_history": [],
            "steps": [],
            "phases": [],
        }
    phases_out = []
    for ph in (phase_rows or []):
        phases_out.append({
            "phase_local_id": ph.get("phase_local_id"),
            "order_index": int(ph.get("order_index") or 0),
            "title": ph.get("title", ""),
            "duration_weeks": ph.get("duration_weeks"),
            "rationale_md": ph.get("rationale_md"),
            "deliverable_md": ph.get("deliverable_md"),
        })
    return {
        "id": path_row.get("path_id") or f"path-{goal_id}",
        "title": path_row.get("title", ""),
        "progress_pct": int(path_row.get("progress_pct") or 0),
        "current_step_id": path_row.get("current_step_id"),
        "estimated_total_minutes": int(path_row.get("estimated_total_minutes") or 0),
        "last_recompute_reason": path_row.get("last_recompute_reason"),
        "last_recomputed_at": _iso(path_row.get("last_recomputed_at")),
        "recompute_history": [
            {
                "date": _iso(h["recomputed_at"])[:10] if h.get("recomputed_at") else "",
                "trigger": h.get("trigger"),
                "reason": h.get("reason"),
                **(h.get("diff") if isinstance(h.get("diff"), dict) else {}),
            }
            for h in history_rows
        ],
        "steps": [_row_to_step(s) for s in step_rows],
        "phases": phases_out,
    }


def _row_to_step(row: dict) -> dict:
    out = {
        "id": row["step_id"],
        "order": float(row["step_order"]) if row.get("step_order") is not None else 0,
        "title": row.get("title", ""),
        "step_type": row.get("step_type", "content"),
        "status": row.get("status", "pending"),
    }
    for opt in ("estimated_minutes", "actual_minutes", "mastery_at_completion",
                "inserted_by", "inserted_reason",
                "content_url", "content_provider", "content_title", "content_id",
                "phase_local_id", "step_rationale", "is_free"):
        v = row.get(opt)
        if v is not None:
            out[opt] = float(v) if opt == "mastery_at_completion" else v
    if row.get("completed_at"):
        out["completed_at"] = _iso(row["completed_at"])[:10]
    return out


def _iso(v):
    if v is None:
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _persist_goal_path(user_id: str, goal_id: str) -> None:
    """
    Upsert goal + path + delete-and-replace path_steps for this goal.
    No-op when Postgres isn't configured (fallback uses _STORE in place).
    """
    if not db.is_enabled():
        return
    entry = _STORE.get(user_id, {}).get(goal_id)
    if not entry:
        return
    g = entry["goal"]
    p = entry["path"]

    try:
        with db.transaction() as cur:
            if cur is None:
                return
            cur.execute(
                """
                INSERT INTO goals
                    (user_id, goal_id, name, objective, timeline, days_left,
                     success_criteria, priority, status, readiness, delta, assigned_by,
                     context_source_type, context_url, context_filename, context_mime, context_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, goal_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    objective = EXCLUDED.objective,
                    timeline = EXCLUDED.timeline,
                    days_left = EXCLUDED.days_left,
                    success_criteria = EXCLUDED.success_criteria,
                    priority = EXCLUDED.priority,
                    status = EXCLUDED.status,
                    readiness = EXCLUDED.readiness,
                    delta = EXCLUDED.delta,
                    assigned_by = EXCLUDED.assigned_by,
                    context_source_type = EXCLUDED.context_source_type,
                    context_url = EXCLUDED.context_url,
                    context_filename = EXCLUDED.context_filename,
                    context_mime = EXCLUDED.context_mime,
                    context_text = EXCLUDED.context_text
                """,
                (
                    user_id, goal_id, g.get("name", ""), g.get("objective"),
                    g.get("timeline"), g.get("days_left"), g.get("success_criteria"),
                    g.get("priority", "secondary"),
                    _coerce_status(g.get("status", "active")),
                    int(g.get("readiness") or 0),
                    g.get("delta"),
                    g.get("assigned_by", "self"),
                    g.get("context_source_type"),
                    g.get("context_url"),
                    g.get("context_filename"),
                    g.get("context_mime"),
                    g.get("context_text"),
                ),
            )
            cur.execute(
                """
                INSERT INTO paths
                    (user_id, goal_id, path_id, title, progress_pct, current_step_id,
                     estimated_total_minutes, last_recompute_reason,
                     last_recomputed_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, goal_id) DO UPDATE SET
                    path_id = EXCLUDED.path_id,
                    title = EXCLUDED.title,
                    progress_pct = EXCLUDED.progress_pct,
                    current_step_id = EXCLUDED.current_step_id,
                    estimated_total_minutes = EXCLUDED.estimated_total_minutes,
                    last_recompute_reason = EXCLUDED.last_recompute_reason,
                    last_recomputed_at = EXCLUDED.last_recomputed_at,
                    status = EXCLUDED.status
                """,
                (
                    user_id, goal_id, p.get("id") or f"path-{goal_id}",
                    p.get("title", ""), int(p.get("progress_pct") or 0),
                    p.get("current_step_id"),
                    int(p.get("estimated_total_minutes") or 0),
                    p.get("last_recompute_reason"),
                    p.get("last_recomputed_at"),
                    "active",
                ),
            )
            # Replace path_phases for this goal (delete first, then re-insert)
            cur.execute(
                "DELETE FROM path_phases WHERE user_id = %s AND goal_id = %s",
                (user_id, goal_id),
            )
            for phase in p.get("phases", []) or []:
                cur.execute(
                    """
                    INSERT INTO path_phases
                        (user_id, goal_id, phase_local_id, order_index, title,
                         duration_weeks, rationale_md, deliverable_md)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id, goal_id,
                        phase.get("phase_local_id") or "",
                        int(phase.get("order_index") or 0),
                        phase.get("title", ""),
                        phase.get("duration_weeks"),
                        phase.get("rationale_md"),
                        phase.get("deliverable_md"),
                    ),
                )
            cur.execute(
                "DELETE FROM path_steps WHERE user_id = %s AND goal_id = %s",
                (user_id, goal_id),
            )
            for step in p.get("steps", []):
                cur.execute(
                    """
                    INSERT INTO path_steps
                        (user_id, goal_id, step_id, step_order, title, step_type,
                         status, estimated_minutes, actual_minutes,
                         mastery_at_completion, inserted_by, inserted_reason,
                         completed_at,
                         content_url, content_provider, content_title, content_id,
                         phase_local_id, step_rationale, is_free)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id, goal_id, step["id"],
                        float(step.get("order") or 0),
                        step.get("title", ""),
                        step.get("step_type", "content"),
                        step.get("status", "pending"),
                        step.get("estimated_minutes"),
                        step.get("actual_minutes"),
                        step.get("mastery_at_completion"),
                        step.get("inserted_by", "engine"),
                        step.get("inserted_reason"),
                        step.get("completed_at"),
                        step.get("content_url"),
                        step.get("content_provider"),
                        step.get("content_title"),
                        step.get("content_id"),
                        step.get("phase_local_id"),
                        step.get("step_rationale"),
                        bool(step.get("is_free")) if step.get("is_free") is not None else None,
                    ),
                )
    except Exception as exc:
        logger.warning("path_engine persist failed for %s/%s (%s)", user_id, goal_id, exc)


def _persist_recompute_pg(user_id: str, goal_id: str, history_entry: dict,
                           trigger: str | None = None) -> None:
    """Append a single row to path_recomputes (the normalized history table)."""
    if not db.is_enabled():
        return
    try:
        diff = {k: v for k, v in history_entry.items()
                if k not in ("date", "trigger", "reason")}
        db.execute(
            """
            INSERT INTO path_recomputes (user_id, goal_id, trigger, reason, diff)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            """,
            (
                user_id, goal_id,
                trigger or history_entry.get("trigger") or "unknown",
                history_entry.get("reason"),
                json.dumps(diff),
            ),
        )
    except Exception as exc:
        logger.warning("path_recompute insert failed (%s)", exc)


def _coerce_status(status: str) -> str:
    """Normalize legacy status values to the constraint-allowed set."""
    if status in ("active", "achieved", "paused", "abandoned", "archived"):
        return status
    if status == "completed":
        return "achieved"
    return "active"


def _slugify(name: str) -> str:
    import re
    s = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return s or f"goal-{int(datetime.utcnow().timestamp())}"


# ──────────────────────────────────────────────────────────────
# Goal CRUD
# ──────────────────────────────────────────────────────────────

def create_goal(user_id: str, goal_input: dict) -> dict:
    """
    Create a goal + an empty live path. Goal_input fields:
      name (required), priority (primary|secondary|exploration|assigned),
      objective, timeline (ISO date | label), success_criteria,
      readiness (0–100, default 0).
    Returns the new goal + path entry. Idempotent on goal_id collision —
    re-creating the same goal_id updates the goal fields, leaves path intact.
    """
    user_data = _ensure_user(user_id)
    name = (goal_input.get("name") or "").strip()
    if not name:
        return {"error": "goal name is required"}

    goal_id = goal_input.get("id") or _slugify(name)
    now = datetime.utcnow().isoformat()
    if goal_id in user_data:
        # Update existing
        user_data[goal_id]["goal"].update({k: v for k, v in goal_input.items() if k != "id"})
        _persist_goal_path(user_id, goal_id)

        # If the existing goal has an empty path (e.g., it was created before
        # the auto-generation feature landed, or a prior generation attempt
        # bombed), regenerate it now. The user just told us about this goal
        # again — they expect to see steps. Skip when the caller explicitly
        # opted out via auto_generate_path: false.
        existing_steps = (user_data[goal_id]["path"] or {}).get("steps") or []
        if not existing_steps and goal_input.get("auto_generate_path", True):
            try:
                _generate_initial_path(user_id, goal_id)
            except Exception as exc:
                logger.warning("Initial path generation on update-existing failed for %s/%s (%s)", user_id, goal_id, exc)

        # Re-read in case _generate_initial_path replaced _STORE[user_id]
        fresh_entry = _STORE.get(user_id, {}).get(goal_id) or user_data[goal_id]
        return {"goal_id": goal_id, "goal": fresh_entry["goal"], "path": fresh_entry["path"], "created": False}

    # Optional context (URL / document / image / pasted text) attached at
    # goal creation. The route handler in app.py runs goal_context.extract()
    # and passes the extracted text on goal_input as context_text /
    # context_source_type / context_url / context_filename / context_mime.
    # When present, _generate_path_via_claude reads it and grounds the
    # path in the actual JD/role description.
    goal = {
        "id": goal_id,
        "name": name,
        "priority": goal_input.get("priority", "primary"),
        "objective": goal_input.get("objective", ""),
        "timeline": goal_input.get("timeline", ""),
        "days_left": goal_input.get("days_left"),
        "success_criteria": goal_input.get("success_criteria", ""),
        "readiness": int(goal_input.get("readiness") or 0),
        "delta": goal_input.get("delta", "new"),
        "status": "active",
        "created_at": now,
        "context_source_type": goal_input.get("context_source_type"),
        "context_url": goal_input.get("context_url"),
        "context_filename": goal_input.get("context_filename"),
        "context_mime": goal_input.get("context_mime"),
        "context_text": goal_input.get("context_text"),
    }
    path = {
        "id": f"path-{goal_id}",
        "title": f"Path to {name}",
        "progress_pct": 0,
        "current_step_id": None,
        "estimated_total_minutes": 0,
        "last_recompute_reason": "Path created — empty until first session or content match.",
        "last_recomputed_at": now,
        "recompute_history": [],
        "steps": [],
    }
    user_data[goal_id] = {"goal": goal, "path": path}
    _persist_goal_path(user_id, goal_id)

    # Auto-generate initial path. The user just told us what they want to
    # achieve — they expect to see a learning path, not an empty pane with
    # a "trigger recompute" instruction. We synchronously run the engine
    # so the API response carries the populated path. Falls back to a
    # goal-aware stub when ANTHROPIC_API_KEY isn't set so demo mode is
    # never empty.
    if goal_input.get("auto_generate_path", True):
        try:
            _generate_initial_path(user_id, goal_id)
        except Exception as exc:
            logger.warning("Initial path generation failed for %s/%s (%s)", user_id, goal_id, exc)

    # CRITICAL: re-read from _STORE (NOT from the local `user_data` variable)
    # because _generate_initial_path → _ensure_user() may have rebuilt
    # _STORE[user_id] from Postgres, leaving our local user_data reference
    # pointing at the pre-generation dict with the empty path. Reading from
    # _STORE always gets the freshest mutated entry. Fall back to user_data
    # if for some reason _STORE doesn't have it.
    fresh_entry = _STORE.get(user_id, {}).get(goal_id) or user_data.get(goal_id)
    return {"goal_id": goal_id, "goal": fresh_entry["goal"], "path": fresh_entry["path"], "created": True}


def _generate_initial_path(user_id: str, goal_id: str) -> None:
    """
    Generate the initial path for a freshly-created goal.

    Calls Claude when configured (real reasoning over goal + objective +
    success criteria + timeline → ordered 8-12 step path). Falls back to
    a goal-aware 4-step starter template otherwise so the user always
    sees a populated path immediately after Create Goal.
    """
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return
    entry = user_data[goal_id]
    goal = entry["goal"]

    steps: list = []
    phases: list = []
    engine_label = "demo template"

    if claude_client.is_live():
        # L4a — try RAG-augmented generation first. Pulls candidates from the
        # content_catalog (Pinecone-backed) and asks Claude to organize them
        # into phases, selecting steps by content_id (no fabricated URLs).
        candidates = _retrieve_catalog_candidates(goal, top_k=80)
        if candidates:
            try:
                phased = _generate_phased_path_via_claude(goal, candidates, user_id=user_id)
                if phased and phased.get("steps"):
                    steps = phased["steps"]
                    phases = phased.get("phases") or []
                    engine_label = "Claude · RAG-augmented"
            except Exception as exc:
                logger.warning("RAG path generation failed (%s) — falling back to legacy", exc)

        # Fallback — legacy single-pass generator (Claude invents URLs, no phases)
        if not steps:
            steps = _generate_path_via_claude(goal, user_id=user_id)
            engine_label = "Claude · legacy"

    if not steps:
        steps = _stub_initial_steps(goal)
        engine_label = "demo template"

    if not steps:
        return  # nothing to do; leave path empty rather than corrupt it

    now = datetime.utcnow().isoformat()
    entry["path"]["steps"] = steps
    entry["path"]["phases"] = phases  # may be []
    entry["path"]["estimated_total_minutes"] = sum(int(s.get("estimated_minutes") or 0) for s in steps)
    # First step is active, the engine has already laid this out
    active_first = next((s for s in steps if s.get("status") == "active"), steps[0] if steps else None)
    entry["path"]["current_step_id"] = active_first["id"] if active_first else None
    phase_summary = f", {len(phases)} phases" if phases else ""
    entry["path"]["last_recompute_reason"] = (
        f"Initial path generated — {len(steps)} steps{phase_summary} from {engine_label} on goal create."
    )
    entry["path"]["last_recomputed_at"] = now

    history_entry = {
        "date": now[:10],
        "trigger": "goal_create",
        "reason": entry["path"]["last_recompute_reason"],
        "added": [s.get("title") for s in steps],
        "modified_count": 0,
    }
    entry["path"]["recompute_history"].insert(0, history_entry)

    _persist_goal_path(user_id, goal_id)
    _persist_recompute_pg(user_id, goal_id, history_entry, trigger="goal_create")


def _compose_learner_profile(user_id: str, current_goal_id: str | None = None) -> str:
    """
    L5 — compact learner-context block injected into every path-gen prompt.

    Pulls from existing services (no new tables, no extra storage):
      - Resume entries (resume.list_journal)        — proven experience + skills
      - Completed path steps across all goals       — what they've already learned
      - Recent work items (work_items.list_items)   — what they're shipping right now
      - Other active goals                           — adjacent paths, avoid duplication

    Output is a ≤2KB markdown block — Claude uses it to:
      - Skip foundational steps the learner has already mastered
      - Pitch step difficulty to actual experience level
      - Avoid recommending things that overlap an active sibling goal
      - Ground language ("you've already shipped X — next is Y")

    Empty/sparse user → empty string (no harm; Path Engine still works).
    """
    if not user_id:
        return ""

    parts: list[str] = ["LEARNER PROFILE (use to skip what they already know):"]

    # 1) Resume — recent entries + categories
    try:
        journal = _resume_svc.list_journal(user_id, limit=15) or {}
        entries = journal.get("entries") or journal.get("journal") or []
    except Exception:
        entries = []
    if entries:
        recent_lines: list[str] = []
        for e in entries[:8]:
            cat = e.get("category") or "skill"
            label = (e.get("title") or e.get("text") or "").strip()[:80]
            if not label:
                continue
            recent_lines.append(f"  - [{cat}] {label}")
        if recent_lines:
            parts.append("Resume (most recent):")
            parts.extend(recent_lines)

    # 2) Skills proven via completed path steps (across ALL goals)
    proven_skills: dict[str, float] = {}  # title → mastery
    try:
        all_goals = _STORE.get(user_id, {})
        for gid, entry in all_goals.items():
            if gid == current_goal_id:
                # Don't bias against the goal we're generating FOR
                continue
            steps = (entry.get("path") or {}).get("steps") or []
            for s in steps:
                if s.get("status") == "done":
                    title = (s.get("title") or "").strip()
                    if not title:
                        continue
                    mastery = float(s.get("mastery_at_completion") or 0.7)
                    if mastery > proven_skills.get(title, 0):
                        proven_skills[title] = mastery
    except Exception:
        pass
    if proven_skills:
        top_proven = sorted(proven_skills.items(), key=lambda kv: -kv[1])[:12]
        parts.append("Already proficient in (from completed steps):")
        parts.extend(f"  - {t} ({int(m*100)}% mastery)" for t, m in top_proven)

    # 3) Recent work items — what they're shipping
    try:
        wi_resp = _work_items_svc.list_items(owner=user_id, limit=10) or {}
        items = wi_resp.get("items") or wi_resp.get("work_items") or []
    except Exception:
        items = []
    if items:
        wi_lines: list[str] = []
        for i in items[:6]:
            title = (i.get("title") or i.get("name") or "").strip()[:80]
            status = i.get("status") or ""
            if not title:
                continue
            wi_lines.append(f"  - {title} [{status}]")
        if wi_lines:
            parts.append("Recent work shipped / in flight:")
            parts.extend(wi_lines)

    # 4) Other active goals (avoid recommending what's covered elsewhere)
    try:
        all_goals = _STORE.get(user_id, {})
        sibling_goals = [
            (entry.get("goal") or {}).get("name", "")
            for gid, entry in all_goals.items()
            if gid != current_goal_id
            and (entry.get("goal") or {}).get("status") in ("active", "in_progress", None)
        ]
        sibling_goals = [g for g in sibling_goals if g]
    except Exception:
        sibling_goals = []
    if sibling_goals:
        parts.append("Other active goals (don't duplicate what these cover):")
        parts.extend(f"  - {g}" for g in sibling_goals[:5])

    if len(parts) == 1:
        return ""  # nothing to add — return empty so prompt stays clean

    return "\n".join(parts)


def _retrieve_catalog_candidates(goal: dict, top_k: int = 80) -> list[dict]:
    """
    Pull top-K catalog candidates relevant to the goal via content_catalog.retrieve().
    Returns the rows enriched with _score (cosine similarity from Pinecone).
    Empty list if catalog is empty or vector index is offline.

    Query composition: goal name + objective + success_criteria + (when present)
    the attached context_text. Skips priority/timeline since those don't affect
    semantic relevance.
    """
    parts = [
        (goal.get("name") or "").strip(),
        (goal.get("objective") or "").strip(),
        (goal.get("success_criteria") or "").strip(),
        (goal.get("context_text") or "").strip(),
    ]
    query = "\n".join(p for p in parts if p)
    if not query:
        return []
    try:
        return content_catalog.retrieve(query_text=query, top_k=top_k)
    except Exception as exc:
        logger.warning("catalog retrieve failed (%s) — RAG path disabled", exc)
        return []


def _generate_phased_path_via_claude(goal: dict, candidates: list[dict], user_id: str | None = None) -> dict | None:
    """
    L4a — RAG-augmented path generation.

    Asks Claude to organize the goal into 3-6 phases, selecting 2-4 steps per
    phase from `candidates` BY content_id. Claude does not invent URLs —
    every step's content_url comes from a real catalog row.

    Returns: { steps: [...], phases: [...] } or None on failure.
        phase shape: {phase_local_id, order_index, title, duration_weeks, rationale_md, deliverable_md}
        step shape:  the same shape as _generate_path_via_claude returns,
                     plus phase_local_id, content_id, step_rationale, is_free
    """
    if not candidates:
        return None

    # Compact candidate listing for the prompt — Claude needs enough to choose
    # well but the prompt has to fit. Title + source + skills + difficulty +
    # is_free + duration is sufficient signal; description is omitted (would
    # blow the token budget at top_k=80).
    cand_lines: list[str] = []
    by_id: dict[int, dict] = {}
    for c in candidates:
        cid = c.get("content_id")
        if not cid:
            continue
        by_id[cid] = c
        skills = ",".join((c.get("skills") or [])[:5])
        cand_lines.append(
            f"  cid={cid} | {c.get('source','')} | {(c.get('title') or '')[:90]} | "
            f"skills=[{skills}] | {c.get('difficulty') or '—'} | "
            f"{'free' if c.get('is_free') else 'paid'} | "
            f"{c.get('duration_minutes') or '?'}min"
        )

    candidate_block = "\n".join(cand_lines)

    system_prompt = (
        "You are a learning path designer. Given a learner's goal and a list of "
        "vetted, real-world learning resources from a curated catalog, you organize "
        "the journey into 3-6 PHASES (e.g. Foundations → Core skill → Specialization → "
        "Application/Capstone), and for each phase you SELECT 2-4 resources from the "
        "candidates by their content_id (cid).\n\n"
        "STRICT RULES:\n"
        "  - You MUST only use content_ids that appear in the candidate list. Never invent.\n"
        "  - Each step references exactly one content_id (the cid number).\n"
        "  - Sequencing within a phase: foundations before specialization.\n"
        "  - Mix free + paid sensibly; if the goal mentions budget constraints prefer free.\n"
        "  - 8-16 steps total across all phases is the right range.\n"
        "  - First step's status is 'active', the rest 'pending'.\n\n"
        "PHASE-LEVEL FIELDS:\n"
        "  - title: 3-7 words, e.g. 'Foundations of LLMs and Agents'\n"
        "  - duration_weeks: realistic span; sum across phases ≤ goal timeline\n"
        "  - rationale_md: 1-2 sentence WHY this phase exists (what gap it closes)\n"
        "  - deliverable_md: 1 sentence concrete artifact the learner ships at end of phase\n\n"
        "STEP-LEVEL FIELDS:\n"
        "  - step_rationale: 1 sentence why THIS resource fits THIS phase (not generic praise)\n"
        "  - estimated_minutes: use the candidate's duration_minutes; if 0 or missing, estimate 60\n\n"
        "Return ONLY a JSON object:\n"
        "{\n"
        "  \"phases\": [\n"
        "    {\"phase_local_id\":\"phase-1\", \"order_index\":1, \"title\":\"...\",\n"
        "     \"duration_weeks\":3, \"rationale_md\":\"...\", \"deliverable_md\":\"...\",\n"
        "     \"steps\":[{\"cid\":42, \"step_rationale\":\"...\"}, ...]\n"
        "    }, ...\n"
        "  ]\n"
        "}\n"
        "No prose, no markdown fences."
    )

    context_text = (goal.get("context_text") or "").strip()
    goal_payload = {
        "goal_name": goal.get("name"),
        "objective": goal.get("objective"),
        "success_criteria": goal.get("success_criteria"),
        "timeline": goal.get("timeline"),
    }

    user_prompt_parts = [
        "GOAL:\n" + json.dumps(goal_payload, indent=2),
        "\n\nCANDIDATES (select from these by cid only — do NOT invent):\n" + candidate_block,
    ]
    profile_block = _compose_learner_profile(user_id, goal.get("id")) if user_id else ""
    if profile_block:
        user_prompt_parts.append("\n\n" + profile_block)
    if context_text:
        user_prompt_parts.append(
            "\n\nATTACHED CONTEXT (primary signal for what the path should cover):\n" + context_text
        )

    response = claude_client._call_claude(
        system=system_prompt,
        messages=[{"role": "user", "content": "".join(user_prompt_parts)}],
        max_tokens=4096,
    )
    if not response:
        return None

    parsed = claude_client._parse_json_response(response, fallback={"phases": []})
    raw_phases = parsed.get("phases") if isinstance(parsed, dict) else []
    if not isinstance(raw_phases, list) or not raw_phases:
        return None

    # Materialize: validate cids, fetch full rows, compose steps + phases
    phases: list[dict] = []
    steps: list[dict] = []
    step_counter = 0

    for p_idx, raw_p in enumerate(raw_phases, start=1):
        if not isinstance(raw_p, dict):
            continue
        phase_local_id = (raw_p.get("phase_local_id") or f"phase-{p_idx}").strip() or f"phase-{p_idx}"
        title = (raw_p.get("title") or "").strip()
        if not title:
            continue
        phases.append({
            "phase_local_id": phase_local_id,
            "order_index": int(raw_p.get("order_index") or p_idx),
            "title": title,
            "duration_weeks": int(raw_p.get("duration_weeks") or 0) or None,
            "rationale_md": (raw_p.get("rationale_md") or "").strip() or None,
            "deliverable_md": (raw_p.get("deliverable_md") or "").strip() or None,
        })
        for raw_s in raw_p.get("steps") or []:
            if not isinstance(raw_s, dict):
                continue
            cid = raw_s.get("cid") or raw_s.get("content_id")
            try:
                cid_int = int(cid)
            except (TypeError, ValueError):
                continue
            row = by_id.get(cid_int)
            if not row:
                # Claude tried to invent. Skip silently — this is what
                # the validation step is here to prevent.
                logger.info("skip step: cid %s not in candidate set", cid_int)
                continue
            step_counter += 1
            est_minutes = int(row.get("duration_minutes") or 0) or 60
            steps.append({
                "id": f"step-rag-{step_counter}",
                "order": step_counter,
                "title": row.get("title") or "(untitled)",
                "step_type": "content",
                "status": "active" if step_counter == 1 else "pending",
                "estimated_minutes": est_minutes,
                "inserted_by": "engine",
                "inserted_reason": (raw_s.get("step_rationale") or "").strip() or "RAG-selected from catalog",
                "content_url": row.get("source_url") or None,
                "content_provider": row.get("source") or None,
                "content_title": row.get("title") or None,
                "content_id": cid_int,
                "phase_local_id": phase_local_id,
                "step_rationale": (raw_s.get("step_rationale") or "").strip() or None,
                "is_free": bool(row.get("is_free")),
            })

    if not steps or not phases:
        return None

    return {"steps": steps, "phases": phases}


def _generate_path_via_claude(goal: dict, user_id: str | None = None) -> list:
    """Real path generation. Returns list of step dicts (or [] on error)."""
    system_prompt = (
        "You are a learning path designer for a working software engineer. "
        "Given a learner's goal, you produce an ordered initial path of "
        "8-12 steps that takes them from foundations to demonstrable proficiency.\n\n"
        "STRUCTURE the path:\n"
        "  - 1-2 foundation/orientation steps (short, 5-15 min — quick wins)\n"
        "  - 4-6 core competency steps (substantial, 30-90 min each)\n"
        "  - 2-3 application/validation steps (project, exam, demonstration)\n\n"
        "QUALITY rules:\n"
        "  - Every step must be specific and actionable, not 'Learn about X'.\n"
        "    Good: 'Build a multi-region failover with Route 53 weighted routing'.\n"
        "    Bad: 'Learn AWS networking'.\n"
        "  - estimated_minutes reflects real focused engagement time.\n"
        "  - step_type: 'content' (default) | 'review' | 'refresher' | 'gap_closure' | 'assignment'\n"
        "  - First step status is 'active'; rest are 'pending'.\n"
        "  - inserted_by: 'engine'; inserted_reason: short rationale ('auto: foundation step before X').\n\n"
        "REAL LEARNING RESOURCE per step (CRITICAL):\n"
        "  - Each step MUST include content_url, content_provider, content_title pointing\n"
        "    at a real, well-known, reputable learning resource that matches the step.\n"
        "  - Prefer canonical / official sources first: official documentation\n"
        "    (kubernetes.io, docs.aws.amazon.com, react.dev, postgresql.org/docs),\n"
        "    AWS Skill Builder, Linux Foundation Training, freeCodeCamp,\n"
        "    Microsoft Learn, Google Cloud Skills Boost, official YouTube channels\n"
        "    (e.g. Anthropic, AWS, Kubernetes), reputable provider courses\n"
        "    (Coursera with named instructor, Pluralsight, A Cloud Guru, Udacity),\n"
        "    well-maintained OSS repos (github.com/...).\n"
        "  - content_url should be a plausible, well-formed URL on a real domain. If\n"
        "    you're uncertain about an exact deep-link, use the canonical landing\n"
        "    page for the resource (the docs root, the course catalog page) rather\n"
        "    than fabricating a fragile path.\n"
        "  - content_provider is a short label: 'kubernetes.io' | 'AWS Skill Builder' |\n"
        "    'Coursera' | 'YouTube' | 'GitHub' | 'official docs' | etc.\n"
        "  - content_title is the resource's own title (may differ from step title;\n"
        "    e.g. step 'Build mTLS with Istio', content_title 'Securing Service Mesh\n"
        "    Traffic with mTLS — Istio docs').\n"
        "  - NEVER invent a fake provider or a URL on a domain that isn't real.\n\n"
        "Return ONLY a JSON object: { \"steps\": [{ id, order, title, step_type, status, "
        "estimated_minutes, inserted_by, inserted_reason, content_url, content_provider, "
        "content_title }] }\n"
        "  - id format: 'step-init-N' where N is 1..len(steps)\n"
        "  - order: 1, 2, 3, ... incrementing\n"
        "No prose, no markdown fences."
    )
    # Pull in any context the learner attached at goal creation (JD URL,
    # PDF, image of role description, etc.). When present, this is the
    # most important grounding signal for the prompt — it turns "what
    # does an SRE need to learn?" into "what does THIS SRE role at THIS
    # company actually need?"
    context_text = (goal.get("context_text") or "").strip()
    context_source = (goal.get("context_source_type") or "").strip()
    context_url = (goal.get("context_url") or "").strip()
    context_filename = (goal.get("context_filename") or "").strip()

    goal_payload = {
        "goal_name": goal.get("name"),
        "objective": goal.get("objective"),
        "success_criteria": goal.get("success_criteria"),
        "timeline": goal.get("timeline"),
        "priority": goal.get("priority"),
    }

    user_prompt_parts = [json.dumps(goal_payload, indent=2)]
    profile_block = _compose_learner_profile(user_id, goal.get("id")) if user_id else ""
    if profile_block:
        user_prompt_parts.append("\n\n" + profile_block)
    if context_text:
        provenance_bits = []
        if context_source:
            provenance_bits.append(f"source_type: {context_source}")
        if context_url:
            provenance_bits.append(f"url: {context_url}")
        if context_filename:
            provenance_bits.append(f"file: {context_filename}")
        provenance = " · ".join(provenance_bits) if provenance_bits else "attached by learner"
        user_prompt_parts.append(
            "\n\n═══════════════════════════════════════════════\n"
            f"ATTACHED CONTEXT ({provenance}):\n"
            "Use this context as the PRIMARY signal for what the path should\n"
            "cover. If it's a job posting, the steps should reflect the\n"
            "specific tech / responsibilities / experience listed. If it's a\n"
            "team brief or role description, ground the path in the team's\n"
            "actual stack and priorities. Quote-back specific phrases from\n"
            "this context in inserted_reason fields where relevant.\n"
            "═══════════════════════════════════════════════\n\n"
            f"{context_text}"
        )
    user_prompt = "".join(user_prompt_parts)

    response = claude_client._call_claude(
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
        max_tokens=2048,
    )
    if not response:
        return []
    parsed = claude_client._parse_json_response(response, fallback={"steps": []})
    raw_steps = parsed.get("steps") if isinstance(parsed, dict) else []
    if not isinstance(raw_steps, list):
        return []

    # Defensive normalization — Claude is well-behaved but never trust
    cleaned: list[dict] = []
    for i, s in enumerate(raw_steps, start=1):
        if not isinstance(s, dict):
            continue
        title = (s.get("title") or "").strip()
        if not title:
            continue
        # Content fields — accept only well-formed URLs (defensive against
        # Claude returning empty strings or invalid URLs). Any of these can
        # be None; the frontend hides the CTA when they are.
        c_url = (s.get("content_url") or "").strip()
        if c_url and not (c_url.startswith("http://") or c_url.startswith("https://")):
            c_url = ""
        c_provider = (s.get("content_provider") or "").strip() or None
        c_title = (s.get("content_title") or "").strip() or None
        cleaned.append({
            "id": s.get("id") or f"step-init-{i}",
            "order": int(s.get("order") or i),
            "title": title,
            "step_type": s.get("step_type") if s.get("step_type") in ("content", "review", "refresher", "gap_closure", "assignment", "synthetic") else "content",
            "status": s.get("status") if s.get("status") in ("active", "pending") else ("active" if i == 1 else "pending"),
            "estimated_minutes": int(s.get("estimated_minutes") or 30),
            "inserted_by": "engine",
            "inserted_reason": s.get("inserted_reason") or "auto: generated for new goal",
            "content_url": c_url or None,
            "content_provider": c_provider,
            "content_title": c_title,
        })
    # Ensure exactly one active step (first one); the rest pending
    active_set = False
    for s in cleaned:
        if s["status"] == "active" and not active_set:
            active_set = True
        elif s["status"] == "active":
            s["status"] = "pending"
    if cleaned and not active_set:
        cleaned[0]["status"] = "active"
    return cleaned


def _stub_initial_steps(goal: dict) -> list:
    """
    Goal-aware starter template for demo mode (no Claude).

    Better than a blank path. Names the steps after the goal so the user
    sees something coherent even without an API key. content_* fields are
    None — stub mode doesn't fabricate URLs. The Path Engine will refine
    these once a real session fires the recompute trigger.
    """
    name = (goal.get("name") or "your goal").strip()
    return [
        {"id": "step-init-1", "order": 1, "title": f"Orient: what does {name} look like?",                "step_type": "content", "status": "active",  "estimated_minutes": 20, "inserted_by": "engine", "inserted_reason": "auto: orientation step (stub mode — set ANTHROPIC_API_KEY to generate a real path)", "content_url": None, "content_provider": None, "content_title": None},
        {"id": "step-init-2", "order": 2, "title": f"Foundations — the core concepts behind {name}",      "step_type": "content", "status": "pending", "estimated_minutes": 45, "inserted_by": "engine", "inserted_reason": "auto: foundations (stub mode)",                "content_url": None, "content_provider": None, "content_title": None},
        {"id": "step-init-3", "order": 3, "title": f"Hands-on practice toward {name}",                    "step_type": "content", "status": "pending", "estimated_minutes": 90, "inserted_by": "engine", "inserted_reason": "auto: applied practice (stub mode)",          "content_url": None, "content_provider": None, "content_title": None},
        {"id": "step-init-4", "order": 4, "title": f"Validation — demonstrate progress toward {name}",    "step_type": "review",  "status": "pending", "estimated_minutes": 60, "inserted_by": "engine", "inserted_reason": "auto: validation step (stub mode)",          "content_url": None, "content_provider": None, "content_title": None},
    ]


def archive_goal(user_id: str, goal_id: str) -> dict:
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}
    user_data[goal_id]["goal"]["status"] = "archived"
    user_data[goal_id]["goal"]["archived_at"] = datetime.utcnow().isoformat()
    _persist_goal_path(user_id, goal_id)
    return {"goal_id": goal_id, "status": "archived"}


def update_goal_progress(user_id: str, goal_id: str, readiness: int = None, delta: str = None) -> dict:
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}
    g = user_data[goal_id]["goal"]
    if readiness is not None:
        prev = g.get("readiness", 0)
        g["readiness"] = max(0, min(100, int(readiness)))
        g["delta"] = delta or (f"+{g['readiness'] - prev} this update" if g['readiness'] != prev else "no change")
    elif delta:
        g["delta"] = delta
    _persist_goal_path(user_id, goal_id)
    return {"goal_id": goal_id, "goal": g}


def queue_assignment(user_id: str, assignment: dict) -> dict:
    """
    Manager assigns content. Queued for the next recompute(assignment_create).
    Returns {queued: bool, queue_size: int}.
    """
    q = _ASSIGNMENT_QUEUE.setdefault(user_id, [])
    q.append({**assignment, "queued_at": datetime.utcnow().isoformat()})
    return {"queued": True, "queue_size": len(q)}


def drain_assignments(user_id: str) -> list:
    return _ASSIGNMENT_QUEUE.pop(user_id, [])


def list_goals(user_id: str) -> dict:
    """Return all active goals + a path summary for each."""
    user_data = _ensure_user(user_id)
    goals = []
    for goal_id, entry in user_data.items():
        path = entry["path"]
        goals.append({
            **entry["goal"],
            "path_summary": {
                "path_id": path["id"],
                "progress_pct": path["progress_pct"],
                "current_step_id": path["current_step_id"],
                "current_step_title": _find_step(path, path["current_step_id"], "title"),
                "total_steps": len(path["steps"]),
                "completed_steps": sum(1 for s in path["steps"] if s["status"] == "done"),
                "last_recompute_reason": path["last_recompute_reason"],
                "last_recomputed_at": path["last_recomputed_at"],
                "recent_adjustments": path["recompute_history"][:2],
            },
        })
    return {
        "user_id": user_id,
        "goal_count": len(goals),
        "goals": goals,
    }


def get_path(user_id: str, goal_id: str) -> dict:
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}
    entry = user_data[goal_id]
    return {
        "goal": entry["goal"],
        "path": entry["path"],
    }


# ──────────────────────────────────────────────────────────────
# Recompute — the engine
# ──────────────────────────────────────────────────────────────

# Deterministic stub diffs per trigger, so the demo tells a coherent story
# even without ANTHROPIC_API_KEY set.
STUB_DIFFS = {
    "session_complete": {
        "summary": "Marked Service Mesh done. Detected mTLS gap during the session. Inserted 'mTLS Quickstart (10 min)' before AWS Core Services.",
        "added": [
            {"id": "step-6a", "order": 6.5, "title": "mTLS Quickstart", "step_type": "gap_closure", "status": "active", "estimated_minutes": 10, "inserted_by": "engine", "inserted_reason": "auto: gap detected during Service Mesh session"},
        ],
        "modified": [
            {"id": "step-6", "status": "done", "mastery_at_completion": 0.7, "actual_minutes": 32, "completed_at": "today"},
        ],
        "removed": [],
        "reordered": [],
    },
    "content_added": {
        "summary": "New content matched your path: 'GitOps with ArgoCD' (high relevance). Inserted as alternative for IaC step.",
        "added": [
            {"id": "step-8a", "order": 8.5, "title": "GitOps with ArgoCD (alternative)", "step_type": "content", "status": "pending", "estimated_minutes": 60, "inserted_by": "engine", "inserted_reason": "auto: new content matched (relevance 0.84)"},
        ],
        "modified": [],
        "removed": [],
        "reordered": [],
    },
    "staleness_flag": {
        "summary": "K8s 1.31 staleness on Service Networking → step marked stale, refresher inserted.",
        "added": [
            {"id": "step-5b", "order": 5.7, "title": "topologySpreadConstraints quickstart", "step_type": "refresher", "status": "active", "estimated_minutes": 5, "inserted_by": "engine", "inserted_reason": "auto: Currency Watch — topologyKeys deprecated"},
        ],
        "modified": [
            {"id": "step-5", "status": "stale"},
        ],
        "removed": [],
        "reordered": [],
    },
    "assignment_create": {
        "summary": "Manager assigned 'AWS Cost Optimization' — inserted into your path before AWS Core Services.",
        "added": [
            {"id": "step-7a", "order": 6.8, "title": "AWS Cost Optimization (manager-assigned)", "step_type": "assignment", "status": "pending", "estimated_minutes": 45, "inserted_by": "manager", "inserted_reason": "manager-assigned — Raj"},
        ],
        "modified": [],
        "removed": [],
        "reordered": [],
    },
    "learner_edit": {
        "summary": "Manual edit applied (engine respects this — won't undo it).",
        "added": [],
        "modified": [],
        "removed": [],
        "reordered": [],
    },
}


def recompute(user_id: str, goal_id: str, trigger: str, trigger_payload: dict = None) -> dict:
    """
    Run the Path Adjustment Engine. Apply the resulting diff to the stored path.
    Returns the diff (so the chat can surface it as a path_update card).

    Triggers: session_complete | content_added | staleness_flag | assignment_create | learner_edit
    """
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}

    entry = user_data[goal_id]
    path = entry["path"]

    # 1. Get the diff — Claude (when live) or stub (when not)
    if claude_client.is_live():
        diff = _engine_via_claude(path, entry["goal"], trigger, trigger_payload or {})
    else:
        diff = _stub_diff(trigger, trigger_payload or {})

    # 2. Apply the diff
    _apply_diff(path, diff)

    # 3. Update path metadata
    now = datetime.utcnow().isoformat()
    path["last_recomputed_at"] = now
    path["last_recompute_reason"] = diff.get("summary", f"recompute: {trigger}")

    # 4. Append to history (most recent first)
    history_entry = {
        "date": now[:10],
        "trigger": trigger,
        "reason": diff.get("summary", ""),
        "added": [s.get("title") for s in diff.get("added", [])],
        "modified_count": len(diff.get("modified", [])),
    }
    path["recompute_history"].insert(0, history_entry)
    path["recompute_history"] = path["recompute_history"][:10]  # cap log

    # 5. Recompute progress + current step + readiness
    prev_readiness = int(entry["goal"].get("readiness") or 0)
    _recompute_progress(path, entry["goal"])
    new_readiness = int(entry["goal"].get("readiness") or 0)

    # 6. Persist back to Postgres (no-op when fallback)
    _persist_goal_path(user_id, goal_id)
    _persist_recompute_pg(user_id, goal_id, history_entry, trigger=trigger)

    # 7. Bounded-change rule — flag diffs that touch >30% of pending steps
    pending_count = max(1, sum(1 for s in path["steps"] if s["status"] == "pending"))
    touched = (
        len(diff.get("added", []) or [])
        + len(diff.get("modified", []) or [])
        + len(diff.get("removed", []) or [])
        + len(diff.get("reordered", []) or [])
    )
    change_pct = round(touched / pending_count, 3)
    requires_confirmation = change_pct > 0.30

    return {
        "goal_id": goal_id,
        "goal_name": entry["goal"]["name"],
        "trigger": trigger,
        "diff": diff,
        "change_pct": change_pct,
        "requires_confirmation": requires_confirmation,
        "path_after": {
            "progress_pct": path["progress_pct"],
            "current_step_id": path["current_step_id"],
            "current_step_title": _find_step(path, path["current_step_id"], "title"),
            "total_steps": len(path["steps"]),
        },
        "readiness_before": prev_readiness,
        "readiness_after": new_readiness,
        "readiness_delta": new_readiness - prev_readiness,
        "recomputed_at": now,
        "mode": "live" if claude_client.is_live() else "stub",
    }


def primary_goal_id(user_id: str):
    """Return the user's primary active goal_id, or None."""
    user_data = _ensure_user(user_id)
    primary = next(
        (gid for gid, e in user_data.items()
         if e["goal"].get("priority") == "primary" and e["goal"].get("status") == "active"),
        None,
    )
    if primary:
        return primary
    return next(
        (gid for gid, e in user_data.items() if e["goal"].get("status") == "active"),
        None,
    )


def find_step_owner(user_id: str, step_id: str):
    """Walk all goals; return goal_id that owns step_id (or None)."""
    user_data = _ensure_user(user_id)
    for gid, entry in user_data.items():
        if any(s["id"] == step_id for s in entry["path"]["steps"]):
            return gid
    return None


def mark_step_done(user_id: str, goal_id: str, step_id: str, mastery: float = None, duration_minutes: int = None) -> dict:
    """
    Used by trigger wiring after /capture/session. Marks the step done with
    mastery + actual duration. Auto-advances current_step_id.
    """
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}
    path = user_data[goal_id]["path"]
    step = next((s for s in path["steps"] if s["id"] == step_id), None)
    if not step:
        return {"error": f"step {step_id} not found"}
    step["status"] = "done"
    step["completed_at"] = datetime.utcnow().isoformat()[:10]
    if mastery is not None:
        step["mastery_at_completion"] = round(float(mastery), 2)
    if duration_minutes is not None:
        step["actual_minutes"] = int(duration_minutes)
    goal = user_data[goal_id]["goal"]
    prev_readiness = int(goal.get("readiness") or 0)
    _recompute_progress(path, goal)
    new_readiness = int(goal.get("readiness") or 0)
    _persist_goal_path(user_id, goal_id)
    return {
        "goal_id": goal_id,
        "step_id": step_id,
        "status": "done",
        "progress_pct": path["progress_pct"],
        "readiness": new_readiness,
        "readiness_delta": new_readiness - prev_readiness,
    }


def skip_step(user_id: str, goal_id: str, step_id: str, reason: str = "") -> dict:
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}
    path = user_data[goal_id]["path"]
    step = next((s for s in path["steps"] if s["id"] == step_id), None)
    if not step:
        return {"error": f"step {step_id} not found"}
    step["status"] = "skipped"
    step["inserted_by"] = "learner"  # mark sacred — engine won't unskip
    step["skipped_reason"] = reason
    step["skipped_at"] = datetime.utcnow().isoformat()
    goal = user_data[goal_id]["goal"]
    _recompute_progress(path, goal)
    history_entry = {
        "date": datetime.utcnow().isoformat()[:10],
        "trigger": "learner_edit",
        "reason": f"Learner skipped: {step.get('title')}" + (f" ({reason})" if reason else ""),
        "added": [],
        "modified_count": 1,
    }
    path["recompute_history"].insert(0, history_entry)
    _persist_goal_path(user_id, goal_id)
    _persist_recompute_pg(user_id, goal_id, history_entry, trigger="learner_edit")
    return {"goal_id": goal_id, "step_id": step_id, "status": "skipped"}


def reorder_step(user_id: str, goal_id: str, step_id: str, new_order: float) -> dict:
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}
    path = user_data[goal_id]["path"]
    step = next((s for s in path["steps"] if s["id"] == step_id), None)
    if not step:
        return {"error": f"step {step_id} not found"}
    step["order"] = float(new_order)
    step["inserted_by"] = "learner"  # learner-touched → sacred
    path["steps"].sort(key=lambda s: s.get("order", 999))
    history_entry = {
        "date": datetime.utcnow().isoformat()[:10],
        "trigger": "learner_edit",
        "reason": f"Learner reordered: {step.get('title')} → position {new_order}",
        "added": [],
        "modified_count": 1,
    }
    path["recompute_history"].insert(0, history_entry)
    _persist_goal_path(user_id, goal_id)
    _persist_recompute_pg(user_id, goal_id, history_entry, trigger="learner_edit")
    return {"goal_id": goal_id, "step_id": step_id, "new_order": new_order}


def insert_step_manual(user_id: str, goal_id: str, step: dict) -> dict:
    """Learner explicitly adds a step. Marked inserted_by=learner — sacred to engine."""
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}

    path = user_data[goal_id]["path"]
    new_step = {
        "id": f"step-manual-{int(datetime.utcnow().timestamp())}",
        "order": step.get("order", len(path["steps"]) + 1),
        "status": "pending",
        "step_type": step.get("step_type", "content"),
        "inserted_by": "learner",
        "inserted_reason": step.get("inserted_reason", "manual learner edit"),
        **step,
    }
    path["steps"].append(new_step)
    path["steps"].sort(key=lambda s: s.get("order", 999))
    history_entry = {
        "date": datetime.utcnow().isoformat()[:10],
        "trigger": "learner_edit",
        "reason": f"Learner inserted: {new_step.get('title')}",
        "added": [new_step.get("title")],
        "modified_count": 0,
    }
    path["recompute_history"].insert(0, history_entry)
    _persist_goal_path(user_id, goal_id)
    _persist_recompute_pg(user_id, goal_id, history_entry, trigger="learner_edit")
    return {"step": new_step, "path_steps_count": len(path["steps"])}


# ──────────────────────────────────────────────────────────────
# Engine internals
# ──────────────────────────────────────────────────────────────

def _engine_via_claude(path: dict, goal: dict, trigger: str, payload: dict) -> dict:
    """Real reasoning path — Claude Sonnet over (path + goal + trigger) → diff."""
    system_prompt = (
        "You are the Path Adjustment Engine for a personal AI learning agent. "
        "Given a learner's current path, their goal, and a trigger event, return a JSON diff. "
        "Rules:\n"
        "  - Manual learner edits (inserted_by='learner') are SACRED — never reorder, modify, or remove them.\n"
        "  - Bounded change: never modify more than 30% of pending steps in one diff.\n"
        "  - Always include a one-line 'summary' explaining what changed and why.\n"
        "  - Diff shape: { summary, added: [step], modified: [{id, ...fields}], removed: [step_id], reordered: [{id, new_order}] }\n"
        "  - Each new step: { id (slug), order, title, step_type (content|review|refresher|gap_closure|assignment), "
        "    status (pending|active), estimated_minutes, inserted_by: 'engine', inserted_reason: 'auto: ...' }\n"
        "Return ONLY the JSON diff."
    )
    user_prompt = (
        f"GOAL:\n{goal}\n\n"
        f"TRIGGER: {trigger}\n"
        f"TRIGGER PAYLOAD:\n{payload}\n\n"
        f"CURRENT PATH (steps in order):\n{[{'id': s['id'], 'order': s['order'], 'title': s['title'], 'status': s['status'], 'inserted_by': s.get('inserted_by', 'engine')} for s in path['steps']]}"
    )

    diff = claude_client._call_claude(
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
        max_tokens=1024,
    )
    parsed = claude_client._parse_json_response(diff, fallback={
        "summary": "Engine returned malformed diff — kept path unchanged.",
        "added": [], "modified": [], "removed": [], "reordered": [],
    })
    return parsed


def _stub_diff(trigger: str, payload: dict) -> dict:
    """Deterministic stub diffs that demonstrate the loop visibly."""
    diff = STUB_DIFFS.get(trigger, {
        "summary": f"[STUB] No diff defined for trigger '{trigger}'.",
        "added": [], "modified": [], "removed": [], "reordered": [],
    })
    return dict(diff)  # shallow copy so callers can't mutate the static dict


def _apply_diff(path: dict, diff: dict):
    """Mutate the path in place per the diff."""
    # Modifications first (status, mastery, etc.)
    for mod in diff.get("modified", []) or []:
        sid = mod.get("id")
        for s in path["steps"]:
            if s["id"] == sid:
                s.update({k: v for k, v in mod.items() if k != "id"})
                break

    # Additions — respect inserted_by=learner sacredness (engine never overrides)
    for new_step in diff.get("added", []) or []:
        if not any(s["id"] == new_step.get("id") for s in path["steps"]):
            path["steps"].append(new_step)

    # Removals — only if NOT learner-inserted (sacred rule)
    for rm_id in diff.get("removed", []) or []:
        path["steps"] = [s for s in path["steps"] if s["id"] != rm_id or s.get("inserted_by") == "learner"]

    # Reordering
    for r in diff.get("reordered", []) or []:
        for s in path["steps"]:
            if s["id"] == r.get("id"):
                s["order"] = r.get("new_order", s["order"])

    path["steps"].sort(key=lambda s: s.get("order", 999))


def _recompute_progress(path: dict, goal: dict | None = None):
    """
    Update progress_pct + current_step_id based on step statuses, and
    auto-promote the next pending step to active when nothing else is.

    When `goal` is passed, also recompute the goal's readiness score using
    the live composite formula. This is the function every state-change
    path goes through (mark_step_done, skip_step, recompute), so passing
    `goal` here means readiness reflects actual learner state instead of
    sitting at whatever the user typed at goal creation.

    The auto-promote rule is what turns "mark step done" into a complete
    user loop instead of leaving the next step in limbo. Sort by step_order
    so promotion respects path ordering, not insertion order (engine
    inserts at fractional orders like 5.5).
    """
    sorted_steps = sorted(path["steps"], key=lambda s: float(s.get("order") or 0))

    completed = sum(1 for s in path["steps"] if s["status"] == "done")
    total = len(path["steps"])
    path["progress_pct"] = int((completed / total) * 100) if total else 0

    # Auto-promote: if no step is currently 'active', flip the first
    # pending step (by order) to active so the user always has a
    # next-action focal point.
    has_active = any(s["status"] == "active" for s in sorted_steps)
    if not has_active:
        next_pending = next((s for s in sorted_steps if s["status"] == "pending"), None)
        if next_pending:
            next_pending["status"] = "active"

    # current_step_id = first 'active' (post-promotion) or first 'pending'
    active = next((s for s in sorted_steps if s["status"] == "active"), None)
    pending = next((s for s in sorted_steps if s["status"] == "pending"), None)
    path["current_step_id"] = (active or pending or {}).get("id", path.get("current_step_id"))

    # Recompute readiness when we have the goal in hand
    if goal is not None:
        new_readiness = _compute_readiness(path, goal)
        prev_readiness = int(goal.get("readiness") or 0)
        if new_readiness != prev_readiness:
            goal["readiness"] = new_readiness
            delta = new_readiness - prev_readiness
            if delta > 0:
                goal["delta"] = f"+{delta} this update"
            elif delta < 0:
                goal["delta"] = f"{delta} this update"
            else:
                goal["delta"] = "no change"


def _compute_readiness(path: dict, goal: dict) -> int:
    """
    Compute a learner's readiness score for a goal as a composite signal,
    on a 0-100 scale (higher = more ready).

    Components:
      • completion (50%) — % of path steps marked done
      • mastery    (30%) — avg mastery_at_completion across done steps
      • momentum   (10%) — recent activity (steps completed in last 30 days
                            relative to the path's typical pace)
      • time_press (10%) — days_left vs estimated remaining minutes

    Each component is on 0-100. Final score is a weighted sum, clamped.

    A path with zero done steps and no mastery captured returns ~0-15
    depending on time pressure; a path mostly complete with high mastery
    pushes 80-95. Calibrated so the demo seed (4 done of 12 with mastery
    ~0.75) reads ~50, matching the "halfway there" intuition.

    Returns int 0-100. Defensive against missing fields, malformed steps.
    """
    steps = path.get("steps") or []
    total = len(steps)
    if total == 0:
        return 0

    done_steps = [s for s in steps if s.get("status") == "done"]

    # 1. Completion — straightforward
    completion = (len(done_steps) / total) * 100

    # 2. Mastery — average across done steps that have mastery captured.
    #    If no mastery has been captured yet but steps are done, use 0.6
    #    as a conservative default (some learning happened, but unmeasured).
    mastered = [
        float(s.get("mastery_at_completion"))
        for s in done_steps
        if s.get("mastery_at_completion") is not None
    ]
    if mastered:
        mastery = (sum(mastered) / len(mastered)) * 100
    elif done_steps:
        mastery = 60.0  # fallback when steps marked done without mastery measure
    else:
        mastery = 0.0

    # 3. Momentum — count steps completed in the last 30d. Treat completing
    #    >=3 in last 30d as full momentum (100), 0 as nothing (0), linear in
    #    between. Steps without completed_at are ignored.
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=30)).date().isoformat()
    recent_done = sum(
        1 for s in done_steps
        if (s.get("completed_at") or "") >= cutoff
    )
    momentum = min(100.0, (recent_done / 3.0) * 100.0)

    # 4. Time pressure — when the goal has a known deadline, score based on
    #    whether remaining estimated time fits in remaining days. If there's
    #    no deadline, treat as neutral (50). Score formula: more buffer time
    #    => higher score (less anxiety inducing).
    days_left = goal.get("days_left")
    pending_steps = [s for s in steps if s.get("status") in ("pending", "active")]
    remaining_minutes = sum(int(s.get("estimated_minutes") or 0) for s in pending_steps)
    if days_left is None:
        time_press = 50.0  # neutral if no deadline
    elif days_left <= 0:
        time_press = 0.0   # past deadline
    else:
        # Assume ~30 min/day sustainable pace; buffer ratio = available / needed
        available_minutes = days_left * 30
        if remaining_minutes <= 0:
            time_press = 100.0
        else:
            ratio = available_minutes / remaining_minutes
            # ratio >= 2 → ample time (100); ratio < 0.5 → severely behind (0)
            time_press = max(0.0, min(100.0, (ratio - 0.5) * (100.0 / 1.5)))

    # Weighted composite
    score = (
        completion * 0.50
        + mastery   * 0.30
        + momentum  * 0.10
        + time_press * 0.10
    )
    return max(0, min(100, int(round(score))))


def _find_step(path: dict, step_id: str, field: str = None):
    s = next((step for step in path["steps"] if step["id"] == step_id), None)
    if s is None:
        return None
    return s.get(field) if field else s
