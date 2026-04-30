"""
Reports v1 — Internal Pilot Pack · Phase D.

Three prebuilt reports for L&D / Org admins:

  1. SKILL COVERAGE BY DEPARTMENT
     For each department, what skills are people learning + how many
     people are working on each skill. Reveals coverage gaps the L&D
     team should commission content for.

  2. PATH COMPLETION
     For each user (filterable by department), counts of path steps
     in each status (pending / active / done / known / skipped /
     stale) plus current readiness %. Identifies stuck learners.

  3. ENGAGEMENT
     Per-user sessions/minutes/active-days over a period. Org-level
     active-user counts (DAU / WAU / MAU). The "are people using it"
     metric.

REPORT SHAPE
────────────
Each function returns:
  {
    report_id:    "skill_coverage" | "path_completion" | "engagement"
    title:        human-readable title
    generated_at: ISO 8601 UTC
    period_days:  int (or None for point-in-time)
    scope:        dict — filters that were applied
    summary:      dict — aggregate stats
    rows:         list of dicts — main data table (CSV-exportable)
    columns:      list of {key, label} — drives the UI table
  }

CSV EXPORT
──────────
The columns array drives both the UI table AND the CSV export, so
adding a column to a report propagates to both surfaces.
"""

import csv
import io
from datetime import datetime, timedelta

from . import rbac, team as team_svc, path_engine


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _all_users():
    """Active users from rbac, with demo seed triggered."""
    rbac._ensure_user("demo-user")
    return [u for u in rbac._USERS.values() if u.get("is_active", True)]


def _normalize_skill(s):
    return (s or "").strip().lower()


# ──────────────────────────────────────────────────────────────
# Report 1 — Skill coverage by department
# ──────────────────────────────────────────────────────────────

def skill_coverage_by_department(department: str = None) -> dict:
    """
    Aggregates the skills people are working on (per their primary goal +
    path step titles + path-engine domain knowledge) grouped by department.

    A row represents (department, skill) — number of people active on
    that skill, sample names, primary-goals that include it.
    """
    users = _all_users()
    if department:
        users = [u for u in users if (u.get("department") or "") == department]

    # Build per-user skill set from team profile cache
    user_skill_map = {}
    for u in users:
        # Use the team module's profile builder to get rich profile if cached
        profile = team_svc._build_report_card(u) if hasattr(team_svc, "_build_report_card") else None
        skills = set()
        if profile:
            goal_name = (profile.get("primary_goal") or {}).get("name", "")
            skills.add(_normalize_skill(goal_name))
            # Path engine: pull step titles for the user's goals
            try:
                user_data = path_engine._STORE.get(u["user_id"]) or {}
                for goal_id, entry in user_data.items():
                    for step in entry.get("path", {}).get("steps", []):
                        if step.get("status") in ("active", "pending", "done"):
                            skills.add(_normalize_skill(step.get("title", "")))
            except Exception:
                pass
            for s in profile.get("recent_sessions", []):
                skills.add(_normalize_skill(s.get("title", "")))
        skills.discard("")
        user_skill_map[u["user_id"]] = (u, skills)

    # Aggregate (department, skill) → list of users
    bucket = {}
    for uid, (u, skills) in user_skill_map.items():
        dept = u.get("department") or "Unassigned"
        for s in skills:
            key = (dept, s)
            bucket.setdefault(key, []).append(u)

    rows = []
    for (dept, skill), people in sorted(bucket.items(), key=lambda kv: -len(kv[1])):
        rows.append({
            "department": dept,
            "skill": skill,
            "people_count": len(people),
            "sample_names": ", ".join(sorted({p.get("name") for p in people if p.get("name")})[:3]),
            "primary_goals_touching": ", ".join(sorted({
                ((team_svc._build_report_card(p) or {}).get("primary_goal") or {}).get("name", "")
                for p in people
            } - {""})[:3]),
        })

    # Summary
    by_dept = {}
    for u in users:
        d = u.get("department") or "Unassigned"
        by_dept[d] = by_dept.get(d, 0) + 1

    return {
        "report_id": "skill_coverage",
        "title": "Skill coverage by department",
        "generated_at": datetime.utcnow().isoformat(),
        "period_days": None,
        "scope": {"department_filter": department},
        "summary": {
            "people_in_scope": len(users),
            "departments": len(by_dept),
            "distinct_skills": len({s for _, ss in user_skill_map.values() for s in ss}),
            "people_by_department": by_dept,
        },
        "rows": rows,
        "columns": [
            {"key": "department", "label": "Department"},
            {"key": "skill", "label": "Skill"},
            {"key": "people_count", "label": "People"},
            {"key": "sample_names", "label": "Sample"},
            {"key": "primary_goals_touching", "label": "Primary goals"},
        ],
    }


# ──────────────────────────────────────────────────────────────
# Report 2 — Path completion
# ──────────────────────────────────────────────────────────────

def path_completion(department: str = None) -> dict:
    """
    Per-user path step counts by status. Identifies stuck learners (high
    pending + low done) and momentum (recent done count).
    """
    users = _all_users()
    if department:
        users = [u for u in users if (u.get("department") or "") == department]

    rows = []
    for u in users:
        uid = u["user_id"]
        store = path_engine._STORE.get(uid) or {}
        if not store:
            # Trigger seed for demo-user; otherwise empty path is OK
            if uid == "demo-user":
                path_engine._ensure_user(uid)
                store = path_engine._STORE.get(uid) or {}

        statuses = {"pending": 0, "active": 0, "done": 0, "known": 0, "skipped": 0, "stale": 0}
        total_steps = 0
        primary_goal_name = ""
        primary_progress_pct = None
        primary_readiness = None
        for goal_id, entry in store.items():
            goal = entry.get("goal", {}) or {}
            path = entry.get("path", {}) or {}
            steps = path.get("steps", []) or []
            total_steps += len(steps)
            for step in steps:
                status = step.get("status", "pending")
                if status in statuses:
                    statuses[status] += 1
            if goal.get("priority") == "primary" or not primary_goal_name:
                primary_goal_name = goal.get("name", primary_goal_name)
                primary_progress_pct = path.get("progress_pct", primary_progress_pct)
                primary_readiness = goal.get("readiness", primary_readiness)

        rows.append({
            "user_id": uid,
            "name": u.get("name", uid),
            "department": u.get("department") or "Unassigned",
            "primary_goal": primary_goal_name or "—",
            "readiness": primary_readiness if primary_readiness is not None else "—",
            "progress_pct": primary_progress_pct if primary_progress_pct is not None else "—",
            "total_steps": total_steps,
            "done": statuses["done"],
            "active": statuses["active"],
            "pending": statuses["pending"],
            "skipped": statuses["skipped"],
            "stale": statuses["stale"],
        })

    # Sort: stuck learners (high pending vs done) first
    def _stuck_score(r):
        return (r.get("pending", 0) - r.get("done", 0))
    rows.sort(key=_stuck_score, reverse=True)

    # Summary
    total_users = len(rows)
    avg_readiness_vals = [r["readiness"] for r in rows if isinstance(r["readiness"], (int, float))]
    avg_readiness = round(sum(avg_readiness_vals) / len(avg_readiness_vals)) if avg_readiness_vals else 0
    summary = {
        "users_in_scope": total_users,
        "avg_readiness": avg_readiness,
        "users_with_active_paths": sum(1 for r in rows if r["total_steps"] > 0),
        "users_potentially_stuck": sum(1 for r in rows if r["pending"] > 5 and r["done"] < 2),
        "total_steps_done": sum(r["done"] for r in rows),
    }

    return {
        "report_id": "path_completion",
        "title": "Path completion",
        "generated_at": datetime.utcnow().isoformat(),
        "period_days": None,
        "scope": {"department_filter": department},
        "summary": summary,
        "rows": rows,
        "columns": [
            {"key": "name", "label": "Learner"},
            {"key": "department", "label": "Department"},
            {"key": "primary_goal", "label": "Primary goal"},
            {"key": "readiness", "label": "Readiness"},
            {"key": "progress_pct", "label": "Progress %"},
            {"key": "done", "label": "Done"},
            {"key": "active", "label": "Active"},
            {"key": "pending", "label": "Pending"},
            {"key": "skipped", "label": "Skipped"},
            {"key": "total_steps", "label": "Total steps"},
        ],
    }


# ──────────────────────────────────────────────────────────────
# Report 3 — Engagement
# ──────────────────────────────────────────────────────────────

def engagement(period_days: int = 30, department: str = None) -> dict:
    """
    Per-user sessions / minutes / active days from the team profile cache
    (Phase 2 reads from a real session log table).
    """
    users = _all_users()
    if department:
        users = [u for u in users if (u.get("department") or "") == department]

    today = datetime.utcnow().date()
    cutoff = today - timedelta(days=period_days)

    rows = []
    for u in users:
        profile = team_svc._build_report_card(u) if hasattr(team_svc, "_build_report_card") else None
        sessions_in_period = []
        if profile:
            for s in profile.get("recent_sessions", []) or []:
                try:
                    sd = datetime.fromisoformat(s["date"]).date() if s.get("date") else None
                except Exception:
                    sd = None
                if sd and sd >= cutoff:
                    sessions_in_period.append(s)

        # Use cached aggregates if no per-session breakdown (default profiles return 0)
        sessions_count = (profile or {}).get("sessions_30d", 0) if period_days >= 30 else len(sessions_in_period)
        minutes_count  = (profile or {}).get("minutes_30d", 0)  if period_days >= 30 else sum(s.get("minutes", 0) for s in sessions_in_period)
        active_days = len({s.get("date") for s in sessions_in_period if s.get("date")}) if sessions_in_period else 0

        last_active = (profile or {}).get("last_active") or u.get("last_active_at")
        try:
            last_active_d = datetime.fromisoformat(last_active).date() if last_active else None
            days_since_last_active = (today - last_active_d).days if last_active_d else None
        except Exception:
            days_since_last_active = None

        rows.append({
            "user_id": u["user_id"],
            "name": u.get("name", u["user_id"]),
            "department": u.get("department") or "Unassigned",
            "role": u.get("role", "learner"),
            "sessions": sessions_count,
            "minutes": minutes_count,
            "active_days": active_days,
            "days_since_last_active": days_since_last_active if days_since_last_active is not None else "—",
            "last_active": last_active or "—",
        })

    rows.sort(key=lambda r: -(r["sessions"] or 0))

    # Summary stats
    total_users = len(rows)
    active_in_period = [r for r in rows if isinstance(r["days_since_last_active"], int) and r["days_since_last_active"] <= period_days]
    dau = sum(1 for r in rows if isinstance(r["days_since_last_active"], int) and r["days_since_last_active"] == 0)
    wau = sum(1 for r in rows if isinstance(r["days_since_last_active"], int) and r["days_since_last_active"] <= 7)
    mau = sum(1 for r in rows if isinstance(r["days_since_last_active"], int) and r["days_since_last_active"] <= 30)
    total_sessions = sum(r["sessions"] for r in rows if isinstance(r["sessions"], int))
    total_minutes = sum(r["minutes"] for r in rows if isinstance(r["minutes"], int))

    return {
        "report_id": "engagement",
        "title": f"Engagement — last {period_days} days",
        "generated_at": datetime.utcnow().isoformat(),
        "period_days": period_days,
        "scope": {"department_filter": department, "period_days": period_days},
        "summary": {
            "users_in_scope": total_users,
            "active_in_period": len(active_in_period),
            "DAU": dau,
            "WAU": wau,
            "MAU": mau,
            "total_sessions": total_sessions,
            "total_minutes": total_minutes,
            "avg_sessions_per_active_user": round(total_sessions / max(len(active_in_period), 1), 1),
        },
        "rows": rows,
        "columns": [
            {"key": "name", "label": "Learner"},
            {"key": "department", "label": "Department"},
            {"key": "role", "label": "Role"},
            {"key": "sessions", "label": "Sessions"},
            {"key": "minutes", "label": "Minutes"},
            {"key": "active_days", "label": "Active days"},
            {"key": "days_since_last_active", "label": "Days since active"},
            {"key": "last_active", "label": "Last active"},
        ],
    }


# ──────────────────────────────────────────────────────────────
# Dispatcher + CSV export
# ──────────────────────────────────────────────────────────────

REPORT_RUNNERS = {
    "skill_coverage":  lambda f: skill_coverage_by_department(department=f.get("department")),
    "path_completion": lambda f: path_completion(department=f.get("department")),
    "engagement":      lambda f: engagement(period_days=int(f.get("period_days", 30)),
                                            department=f.get("department")),
}


def run(report_id: str, filters: dict = None) -> dict:
    runner = REPORT_RUNNERS.get(report_id)
    if not runner:
        return {"error": f"unknown report '{report_id}'. Available: {sorted(REPORT_RUNNERS.keys())}"}
    return runner(filters or {})


def export_csv(report_id: str, filters: dict = None) -> str:
    """Return CSV string of a report's rows using the report's own columns spec."""
    result = run(report_id, filters)
    if "error" in result:
        return f"# error: {result['error']}\n"

    columns = result.get("columns", [])
    rows = result.get("rows", [])

    buf = io.StringIO()
    writer = csv.writer(buf)
    # Header: report metadata
    writer.writerow([f"# Aasan report: {result['title']}"])
    writer.writerow([f"# generated_at: {result['generated_at']}"])
    if result.get("period_days"):
        writer.writerow([f"# period_days: {result['period_days']}"])
    writer.writerow([])

    # Column headers
    writer.writerow([c["label"] for c in columns])
    for row in rows:
        writer.writerow([row.get(c["key"], "") for c in columns])

    return buf.getvalue()


def list_reports() -> dict:
    """Used by the UI to render the report selector."""
    return {
        "reports": [
            {"id": "skill_coverage",  "title": "Skill coverage by department",
             "description": "What skills are people learning, grouped by department. Surfaces coverage gaps."},
            {"id": "path_completion", "title": "Path completion",
             "description": "Per-user path step counts by status. Identifies stuck learners and momentum."},
            {"id": "engagement",      "title": "Engagement",
             "description": "Sessions / minutes / active-days per user. DAU / WAU / MAU. The 'are people using it' metric."},
        ],
    }
