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

PHASE 1 STORAGE
───────────────
In-memory dict keyed by user_id (mirrors content_index). Phase 2 migrates
to Airtable Tables 16 (Learning_Paths) + 17 (Path_Steps). The data shape
defined here matches the target schema.

CLAUDE MODE
───────────
When ANTHROPIC_API_KEY is set, the engine prompts Claude Sonnet over
(current_path + employee_state + trigger) to produce a structured diff.
When unset, returns deterministic stub diffs that demonstrate the loop.
"""

from datetime import datetime
from . import claude_client


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


# In-memory store: { user_id: { goal_id: { goal: {...}, path: {...} } } }
_STORE = {}

# Manager-assigned content waiting to be applied — flushed by recompute.
_ASSIGNMENT_QUEUE = {}  # { user_id: [{title, source, url, assigned_by, ...}, ...] }


def _ensure_user(user_id: str):
    """
    Demo-user gets the 3-goal seed for the canned product story.
    Everyone else starts empty — goals come from /goal/create.
    """
    if user_id not in _STORE:
        if user_id == "demo-user":
            _STORE[user_id] = _seed_paths()
        else:
            _STORE[user_id] = {}
    return _STORE[user_id]


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
        return {"goal_id": goal_id, "goal": user_data[goal_id]["goal"], "path": user_data[goal_id]["path"], "created": False}

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
    return {"goal_id": goal_id, "goal": goal, "path": path, "created": True}


def archive_goal(user_id: str, goal_id: str) -> dict:
    user_data = _ensure_user(user_id)
    if goal_id not in user_data:
        return {"error": f"goal {goal_id} not found"}
    user_data[goal_id]["goal"]["status"] = "archived"
    user_data[goal_id]["goal"]["archived_at"] = datetime.utcnow().isoformat()
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
    path["recompute_history"].insert(0, {
        "date": now[:10],
        "trigger": trigger,
        "reason": diff.get("summary", ""),
        "added": [s.get("title") for s in diff.get("added", [])],
        "modified_count": len(diff.get("modified", [])),
    })
    path["recompute_history"] = path["recompute_history"][:10]  # cap log

    # 5. Recompute progress + current step
    _recompute_progress(path)

    # 6. Bounded-change rule — flag diffs that touch >30% of pending steps
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
    _recompute_progress(path)
    return {"goal_id": goal_id, "step_id": step_id, "status": "done", "progress_pct": path["progress_pct"]}


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
    _recompute_progress(path)
    path["recompute_history"].insert(0, {
        "date": datetime.utcnow().isoformat()[:10],
        "trigger": "learner_edit",
        "reason": f"Learner skipped: {step.get('title')}" + (f" ({reason})" if reason else ""),
        "added": [],
        "modified_count": 1,
    })
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
    path["recompute_history"].insert(0, {
        "date": datetime.utcnow().isoformat()[:10],
        "trigger": "learner_edit",
        "reason": f"Learner reordered: {step.get('title')} → position {new_order}",
        "added": [],
        "modified_count": 1,
    })
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
    path["recompute_history"].insert(0, {
        "date": datetime.utcnow().isoformat()[:10],
        "trigger": "learner_edit",
        "reason": f"Learner inserted: {new_step.get('title')}",
        "added": [new_step.get("title")],
        "modified_count": 0,
    })
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


def _recompute_progress(path: dict):
    """Update progress_pct + current_step_id based on step statuses."""
    completed = sum(1 for s in path["steps"] if s["status"] == "done")
    total = len(path["steps"])
    path["progress_pct"] = int((completed / total) * 100) if total else 0

    # Current step = first 'active', else first 'pending'
    active = next((s for s in path["steps"] if s["status"] == "active"), None)
    pending = next((s for s in path["steps"] if s["status"] == "pending"), None)
    path["current_step_id"] = (active or pending or {}).get("id", path.get("current_step_id"))


def _find_step(path: dict, step_id: str, field: str = None):
    s = next((step for step in path["steps"] if step["id"] == step_id), None)
    if s is None:
        return None
    return s.get(field) if field else s
