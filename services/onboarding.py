"""
Onboarding paths by role — Internal Pilot Pack · Phase F.

When a new learner is provisioned (CSV import OR SCIM Phase G), auto-create
a starter goal + learning path tailored to their *job role* (not their
RBAC role — see job_role field on users).

DESIGN
──────
- `ROLE_TEMPLATES` is a dict keyed by job_role slug. Each template has:
    goal: { name, objective, timeline, success_criteria, priority }
    steps: [ { title, step_type, estimated_minutes, inserted_reason }, ... ]
    milestones: [ { day, title, gate } ]   # 30/60/90 markers (Phase G+ uses these)
- `apply_onboarding(user_id, job_role)`:
    1. Picks the template (falls back to "general")
    2. Creates the goal via path_engine.create_goal
    3. Inserts each step via path_engine.insert_step_manual (so they're
       marked inserted_by="learner" and the engine treats them as sacred —
       won't auto-delete)
    4. Returns the goal_id + step count
- Idempotent: re-running for the same user is a no-op (goal already exists).

ADMIN OVERRIDE
──────────────
`set_template(role, template_dict)` lets org_admin customize the templates
for their org. Persists in `_TEMPLATES_OVERRIDE` (Phase 2: Airtable).
"""

from datetime import datetime
from . import path_engine


# ──────────────────────────────────────────────────────────────
# Default templates — sensible starting paths per job_role.
# Phase D: org_admin can override via Admin Console.
# Each step has a step_type that maps to the Path Engine vocabulary.
# ──────────────────────────────────────────────────────────────

DEFAULT_TEMPLATES = {
    "software_engineer": {
        "label": "Software Engineer",
        "description": "Joining the engineering org — first 90 days.",
        "goal": {
            "name": "Engineering ramp — first 90 days",
            "objective": "Be productive on the codebase by Day 30; ship your first independent feature by Day 60; lead a project by Day 90.",
            "timeline": "90 days",
            "success_criteria": "First PR merged Week 2 · solo feature shipped by Day 60 · positive 30/60/90 review feedback",
            "priority": "primary",
            "readiness": 10,
        },
        "steps": [
            {"title": "Engineering onboarding deck",                "step_type": "content",     "estimated_minutes": 60, "inserted_reason": "onboarding template — Day 1"},
            {"title": "Local dev environment setup",                "step_type": "exercise",    "estimated_minutes": 90, "inserted_reason": "onboarding template — Day 1"},
            {"title": "Codebase architecture overview",             "step_type": "content",     "estimated_minutes": 45, "inserted_reason": "onboarding template — Week 1"},
            {"title": "First PR — fix a documented good-first-issue","step_type": "exercise",    "estimated_minutes": 120,"inserted_reason": "onboarding template — Week 2"},
            {"title": "Internal API conventions",                   "step_type": "reference",   "estimated_minutes": 30, "inserted_reason": "onboarding template — Week 2"},
            {"title": "Production deploy + on-call procedures",     "step_type": "content",     "estimated_minutes": 45, "inserted_reason": "onboarding template — Week 4"},
            {"title": "First feature scoping meeting",              "step_type": "review",      "estimated_minutes": 60, "inserted_reason": "onboarding template — Day 30 milestone"},
            {"title": "Solo feature delivery",                      "step_type": "exercise",    "estimated_minutes": 480,"inserted_reason": "onboarding template — Day 60 milestone"},
            {"title": "Lead a small project (2-3 collaborators)",   "step_type": "exercise",    "estimated_minutes": 600,"inserted_reason": "onboarding template — Day 90 milestone"},
        ],
        "milestones": [
            {"day": 30, "title": "Day-30 check-in", "gate": "First PR merged + dev env productive"},
            {"day": 60, "title": "Day-60 review",   "gate": "Solo feature in production"},
            {"day": 90, "title": "Day-90 review",   "gate": "Leading a small project"},
        ],
    },
    "manager": {
        "label": "Engineering Manager",
        "description": "New (or newly-promoted) engineering manager.",
        "goal": {
            "name": "First-time manager ramp",
            "objective": "Effective 1-on-1 cadence by Week 4; team trusts your judgment by Day 60; calibrate first review cycle.",
            "timeline": "120 days",
            "success_criteria": "Team retention 100% · 1-on-1 cadence weekly · positive upward feedback in first skip-level",
            "priority": "primary",
            "readiness": 15,
        },
        "steps": [
            {"title": "Eng manager 1-on-1 playbook",               "step_type": "content",  "estimated_minutes": 45, "inserted_reason": "onboarding template — Week 1"},
            {"title": "First 1-on-1 with each direct report",      "step_type": "exercise", "estimated_minutes": 240,"inserted_reason": "onboarding template — Week 1"},
            {"title": "SBI / COIN feedback frameworks",            "step_type": "content",  "estimated_minutes": 30, "inserted_reason": "onboarding template — Week 2"},
            {"title": "Team retro facilitation",                   "step_type": "exercise", "estimated_minutes": 60, "inserted_reason": "onboarding template — Week 3"},
            {"title": "Performance calibration walkthrough",       "step_type": "content",  "estimated_minutes": 60, "inserted_reason": "onboarding template — Week 6"},
            {"title": "Career laddering for ICs",                  "step_type": "reference","estimated_minutes": 45, "inserted_reason": "onboarding template — Week 8"},
            {"title": "Day-60 skip-level conversation",            "step_type": "review",   "estimated_minutes": 30, "inserted_reason": "onboarding template — Day 60"},
            {"title": "Run your first review cycle",               "step_type": "exercise", "estimated_minutes": 480,"inserted_reason": "onboarding template — Day 120"},
        ],
        "milestones": [
            {"day": 14, "title": "Day-14 check",     "gate": "1-on-1 cadence established with all reports"},
            {"day": 60, "title": "Day-60 review",    "gate": "Skip-level feedback positive"},
            {"day": 120,"title": "Day-120 milestone","gate": "First review cycle calibrated"},
        ],
    },
    "data_engineer": {
        "label": "Data Engineer",
        "description": "Joining the data platform team.",
        "goal": {
            "name": "Data Engineer ramp — first 90 days",
            "objective": "Productive on our data infra by Day 30; own a pipeline by Day 60; design a new data product by Day 90.",
            "timeline": "90 days",
            "success_criteria": "First pipeline shipped · on-call rotation entry · designed one new data product",
            "priority": "primary",
            "readiness": 10,
        },
        "steps": [
            {"title": "Data platform overview",                    "step_type": "content",  "estimated_minutes": 60, "inserted_reason": "onboarding template — Day 1"},
            {"title": "Warehouse + lake architecture",             "step_type": "reference","estimated_minutes": 45, "inserted_reason": "onboarding template — Week 1"},
            {"title": "Pipeline framework + scheduler",            "step_type": "content",  "estimated_minutes": 60, "inserted_reason": "onboarding template — Week 1"},
            {"title": "Data quality + monitoring conventions",     "step_type": "content",  "estimated_minutes": 30, "inserted_reason": "onboarding template — Week 2"},
            {"title": "Ship a small ETL fix",                      "step_type": "exercise", "estimated_minutes": 180,"inserted_reason": "onboarding template — Week 3"},
            {"title": "Data privacy + GDPR refresher",             "step_type": "review",   "estimated_minutes": 30, "inserted_reason": "onboarding template — Week 4 (compliance)"},
            {"title": "Own an existing pipeline",                  "step_type": "exercise", "estimated_minutes": 480,"inserted_reason": "onboarding template — Day 60"},
            {"title": "Design a new data product",                 "step_type": "exercise", "estimated_minutes": 480,"inserted_reason": "onboarding template — Day 90"},
        ],
        "milestones": [
            {"day": 30, "title": "Day-30 check", "gate": "First ETL fix shipped"},
            {"day": 60, "title": "Day-60 review","gate": "Owning an existing pipeline"},
            {"day": 90, "title": "Day-90 review","gate": "New data product designed"},
        ],
    },
    "product_manager": {
        "label": "Product Manager",
        "description": "Joining or rotating into product management.",
        "goal": {
            "name": "PM ramp — first 90 days",
            "objective": "Understand the product + market by Day 30; ship a feature by Day 60; lead a roadmap line by Day 90.",
            "timeline": "90 days",
            "success_criteria": "Roadmap line owned · 5+ customer interviews completed · spec-reviewed by lead",
            "priority": "primary",
            "readiness": 10,
        },
        "steps": [
            {"title": "Product context — strategy + competitive landscape", "step_type": "content",  "estimated_minutes": 90, "inserted_reason": "onboarding template — Week 1"},
            {"title": "Customer interview techniques",                      "step_type": "content",  "estimated_minutes": 45, "inserted_reason": "onboarding template — Week 1"},
            {"title": "First 5 customer interviews",                        "step_type": "exercise", "estimated_minutes": 300,"inserted_reason": "onboarding template — Weeks 2-3"},
            {"title": "Spec writing template + examples",                   "step_type": "reference","estimated_minutes": 30, "inserted_reason": "onboarding template — Week 4"},
            {"title": "Ship a small feature end-to-end",                    "step_type": "exercise", "estimated_minutes": 480,"inserted_reason": "onboarding template — Day 60"},
            {"title": "Take ownership of one roadmap line",                 "step_type": "exercise", "estimated_minutes": 600,"inserted_reason": "onboarding template — Day 90"},
        ],
        "milestones": [
            {"day": 30, "title": "Day-30 check", "gate": "5 customer interviews complete"},
            {"day": 60, "title": "Day-60 review","gate": "Small feature shipped"},
            {"day": 90, "title": "Day-90 review","gate": "Roadmap line owned"},
        ],
    },
    "general": {
        "label": "General — any role",
        "description": "Default starter path for any new hire — a baseline path covering company orientation, tools, and compliance.",
        "goal": {
            "name": "Welcome to the team — first 30 days",
            "objective": "Get oriented to the company, tools, and people. Complete required compliance training.",
            "timeline": "30 days",
            "success_criteria": "Compliance trainings done · 1-on-1 with manager · attended one all-hands",
            "priority": "primary",
            "readiness": 10,
        },
        "steps": [
            {"title": "Welcome — company values + history", "step_type": "content",  "estimated_minutes": 30, "inserted_reason": "onboarding template — Day 1"},
            {"title": "Tools + accounts walkthrough",        "step_type": "exercise", "estimated_minutes": 60, "inserted_reason": "onboarding template — Day 1"},
            {"title": "Data Privacy Compliance 2026",         "step_type": "review",   "estimated_minutes": 30, "inserted_reason": "onboarding template — Week 1 (mandatory)"},
            {"title": "First 1-on-1 with your manager",      "step_type": "exercise", "estimated_minutes": 30, "inserted_reason": "onboarding template — Week 1"},
            {"title": "Attend the next company all-hands",   "step_type": "exercise", "estimated_minutes": 60, "inserted_reason": "onboarding template — Week 2-4"},
            {"title": "Set your primary career goal",        "step_type": "review",   "estimated_minutes": 20, "inserted_reason": "onboarding template — Week 4"},
        ],
        "milestones": [
            {"day": 7,  "title": "Day-7 check",  "gate": "Compliance training done"},
            {"day": 30, "title": "Day-30 check", "gate": "Primary career goal set"},
        ],
    },
}


# Org-admin overrides. { job_role: template_dict }
_TEMPLATES_OVERRIDE = {}


def get_template(job_role: str) -> dict:
    """Resolve a job_role to its template (override > default > general fallback)."""
    if job_role in _TEMPLATES_OVERRIDE:
        return _TEMPLATES_OVERRIDE[job_role]
    if job_role in DEFAULT_TEMPLATES:
        return DEFAULT_TEMPLATES[job_role]
    return DEFAULT_TEMPLATES["general"]


def list_templates() -> dict:
    """For the admin UI — every available template + which are overridden."""
    out = []
    seen = set()
    for slug, tpl in DEFAULT_TEMPLATES.items():
        is_overridden = slug in _TEMPLATES_OVERRIDE
        active = _TEMPLATES_OVERRIDE.get(slug, tpl)
        out.append({
            "slug": slug,
            "label": active.get("label", slug),
            "description": active.get("description", ""),
            "step_count": len(active.get("steps", [])),
            "milestones": len(active.get("milestones", [])),
            "overridden": is_overridden,
        })
        seen.add(slug)
    # any custom-only template (override that doesn't shadow a default)
    for slug, tpl in _TEMPLATES_OVERRIDE.items():
        if slug in seen:
            continue
        out.append({
            "slug": slug, "label": tpl.get("label", slug),
            "description": tpl.get("description", ""),
            "step_count": len(tpl.get("steps", [])),
            "milestones": len(tpl.get("milestones", [])),
            "overridden": True,
        })
    return {"templates": out, "count": len(out)}


def get_template_full(job_role: str) -> dict:
    """Full template body — for the editor."""
    tpl = get_template(job_role)
    return {
        "slug": job_role if job_role in DEFAULT_TEMPLATES or job_role in _TEMPLATES_OVERRIDE else "general",
        "template": tpl,
        "is_overridden": job_role in _TEMPLATES_OVERRIDE,
    }


def set_template(actor_role_check_fn, job_role: str, template: dict) -> dict:
    """Org-admin override. actor_role_check_fn returns True if allowed."""
    if not actor_role_check_fn():
        return {"error": "forbidden — admin:users / admin:modules required"}
    required = ["goal", "steps"]
    for k in required:
        if k not in template:
            return {"error": f"template missing required key: {k}"}
    _TEMPLATES_OVERRIDE[job_role] = template
    return {"ok": True, "slug": job_role, "step_count": len(template.get("steps", []))}


def apply_onboarding(user_id: str, job_role: str = None) -> dict:
    """
    Create a goal + path steps for the user via path_engine.
    Idempotent: returns existing goal_id if user already has one matching the
    template's goal_name.
    """
    if not user_id:
        return {"error": "user_id required"}
    job_role = (job_role or "general").lower().strip()
    tpl = get_template(job_role)

    goal_input = dict(tpl["goal"])

    # Idempotency: skip if a goal with this name already exists for the user
    user_goals = path_engine._STORE.get(user_id) or {}
    for gid, entry in user_goals.items():
        if (entry.get("goal", {}).get("name") or "").lower() == goal_input.get("name", "").lower():
            return {
                "ok": True, "skipped_reason": "user already has this onboarding goal",
                "goal_id": gid, "user_id": user_id, "job_role": job_role,
            }

    create_result = path_engine.create_goal(user_id, goal_input)
    if "error" in create_result:
        return {"error": create_result["error"]}
    goal_id = create_result["goal_id"]

    inserted = []
    for i, step in enumerate(tpl.get("steps", [])):
        step_input = dict(step)
        step_input["order"] = i + 1
        # Mark this step as inserted_by template (we want the engine to NOT
        # treat them as sacred — so the engine can refine/re-order based
        # on session triggers). Without this, learner_edit semantics would
        # block path engine adjustments.
        step_input["inserted_by"] = "onboarding_template"
        result = path_engine.insert_step_manual(user_id, goal_id, step_input)
        if not result.get("error"):
            inserted.append(result.get("step", {}).get("id"))

    return {
        "ok": True,
        "user_id": user_id,
        "job_role": job_role,
        "goal_id": goal_id,
        "goal_name": goal_input.get("name"),
        "steps_inserted": len(inserted),
        "step_ids": inserted,
        "milestones": tpl.get("milestones", []),
    }
