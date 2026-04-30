"""
Team module — manager-facing view of team learning progress.

Phase B (Internal Pilot Pack): list_team queries the org graph in
services/rbac (manager_user_id field) instead of a hardcoded demo team.
The 5 demo personas (priya / david / alex / maya / jordan) keep their
rich profile data via _DEMO_PROFILE_CACHE so the demo stays impressive;
any newly-imported team member gets a sensible default profile.

Each team member is a "report" with a primary goal, path summary, recent
sessions, gaps, and a status (on_track / behind / blocked / exploring /
mandatory). Manager sees these as cards on the Team canvas.
"""

from datetime import datetime, timedelta
from . import rbac


# ──────────────────────────────────────────────────────────────
# Demo profile cache — rich learning state for the 5 demo personas.
# Keyed by user_id. Phase B looks up by user_id whenever a report is
# in this dict; otherwise generates a default profile from rbac record.
# Real production: this data comes from path_engine + resume + sessions.
# ──────────────────────────────────────────────────────────────

def _demo_team():
    today = datetime.utcnow().date()
    return [
        {
            "user_id": "priya-singh",
            "name": "Priya Singh",
            "email": "priya.singh@example.com",
            "role": "Senior Software Engineer",
            "team": "Platform Engineering",
            "primary_goal": {
                "name": "AWS Solutions Architect Pro",
                "priority": "primary",
                "timeline": "Q3 2026",
                "days_left": 92,
                "readiness": 72,
                "readiness_delta": "+8 last 30d",
            },
            "status": "on_track",
            "status_label": "On track",
            "last_active": (today - timedelta(days=1)).isoformat(),
            "sessions_30d": 14,
            "minutes_30d": 480,
            "completed_steps_30d": 6,
            "gaps_count": 2,
            "current_step": "Multi-region patterns",
            "recent_sessions": [
                {"date": (today - timedelta(days=1)).isoformat(), "title": "VPC peering deep dive", "minutes": 35, "mastery": 0.78},
                {"date": (today - timedelta(days=3)).isoformat(), "title": "Multi-region failover patterns", "minutes": 45, "mastery": 0.72},
                {"date": (today - timedelta(days=5)).isoformat(), "title": "AWS IAM cross-account", "minutes": 30, "mastery": 0.85},
            ],
            "gaps": ["FinOps", "Multi-region cost optimization"],
            "kudos_count": 3,
        },
        {
            "user_id": "david-kim",
            "name": "David Kim",
            "email": "david.kim@example.com",
            "role": "Site Reliability Engineer",
            "team": "Platform Engineering",
            "primary_goal": {
                "name": "Kubernetes Specialist",
                "priority": "primary",
                "timeline": "Q4 2026",
                "days_left": 184,
                "readiness": 48,
                "readiness_delta": "-2 last 30d",
            },
            "status": "behind",
            "status_label": "Behind — needs help",
            "last_active": (today - timedelta(days=8)).isoformat(),
            "sessions_30d": 4,
            "minutes_30d": 110,
            "completed_steps_30d": 1,
            "gaps_count": 5,
            "current_step": "Service Mesh — stuck on mTLS",
            "recent_sessions": [
                {"date": (today - timedelta(days=8)).isoformat(), "title": "Service Mesh basics", "minutes": 25, "mastery": 0.45, "flagged": "asked 4 clarifying questions on mTLS — struggle detected"},
                {"date": (today - timedelta(days=14)).isoformat(), "title": "Pod networking", "minutes": 35, "mastery": 0.55},
            ],
            "gaps": ["mTLS", "Networking", "Service Mesh", "Istio config", "Multi-cluster"],
            "kudos_count": 0,
            "manager_attention_flag": "Hasn't completed a session in 8 days. Stuck on mTLS — could benefit from an SME match.",
        },
        {
            "user_id": "alex-rivera",
            "name": "Alex Rivera",
            "email": "alex.rivera@example.com",
            "role": "Senior Software Engineer",
            "team": "Platform Engineering",
            "primary_goal": {
                "name": "Eng Manager Track",
                "priority": "exploration",
                "timeline": "2027",
                "days_left": 580,
                "readiness": 35,
                "readiness_delta": "+5 last 30d",
            },
            "status": "exploring",
            "status_label": "Exploring",
            "last_active": (today - timedelta(days=2)).isoformat(),
            "sessions_30d": 8,
            "minutes_30d": 220,
            "completed_steps_30d": 3,
            "gaps_count": 3,
            "current_step": "1-on-1 cadence + feedback frameworks",
            "recent_sessions": [
                {"date": (today - timedelta(days=2)).isoformat(), "title": "SBI feedback model", "minutes": 25, "mastery": 0.65},
                {"date": (today - timedelta(days=6)).isoformat(), "title": "Eng manager 1-1 playbook", "minutes": 40, "mastery": 0.7},
            ],
            "gaps": ["Performance calibration", "Career laddering", "Hiring rubrics"],
            "kudos_count": 1,
        },
        {
            "user_id": "maya-patel",
            "name": "Maya Patel",
            "email": "maya.patel@example.com",
            "role": "Senior Software Engineer",
            "team": "Platform Engineering",
            "primary_goal": {
                "name": "Data Privacy Compliance 2026",
                "priority": "assigned",
                "timeline": "June 30, 2026",
                "days_left": 61,
                "readiness": 65,
                "readiness_delta": "+25 last 30d",
            },
            "status": "mandatory",
            "status_label": "Compliance — on track",
            "last_active": (today - timedelta(days=3)).isoformat(),
            "sessions_30d": 5,
            "minutes_30d": 75,
            "completed_steps_30d": 2,
            "gaps_count": 0,
            "current_step": "Acknowledgment + recall check",
            "recent_sessions": [
                {"date": (today - timedelta(days=3)).isoformat(), "title": "Data classification refresher", "minutes": 10, "mastery": 0.85},
                {"date": (today - timedelta(days=10)).isoformat(), "title": "PII handling for engineers", "minutes": 30, "mastery": 0.72},
            ],
            "gaps": [],
            "kudos_count": 2,
        },
        {
            "user_id": "jordan-lee",
            "name": "Jordan Lee",
            "email": "jordan.lee@example.com",
            "role": "Software Engineer II",
            "team": "Platform Engineering",
            "primary_goal": {
                "name": "MLOps fundamentals",
                "priority": "exploration",
                "timeline": "No deadline",
                "days_left": None,
                "readiness": 25,
                "readiness_delta": "new",
            },
            "status": "exploring",
            "status_label": "Just started",
            "last_active": (today - timedelta(days=4)).isoformat(),
            "sessions_30d": 3,
            "minutes_30d": 95,
            "completed_steps_30d": 1,
            "gaps_count": 4,
            "current_step": "Model serving fundamentals",
            "recent_sessions": [
                {"date": (today - timedelta(days=4)).isoformat(), "title": "What is MLOps — overview", "minutes": 30, "mastery": 0.55},
            ],
            "gaps": ["Feature stores", "Model monitoring", "Drift detection", "Serving infra"],
            "kudos_count": 0,
        },
    ]


# In-memory kudos log + manager assignments. Phase 2: Airtable.
_KUDOS_LOG = []


# Index demo profiles by user_id once at module load — these are the rich
# learning-state caches for the 5 demo personas. Newly-imported team
# members not in this dict get a default profile via _default_profile().
_DEMO_PROFILE_CACHE = {p["user_id"]: p for p in _demo_team()}


def _default_profile(user_record: dict) -> dict:
    """Sensible default for any report not in the demo profile cache."""
    return {
        "user_id": user_record["user_id"],
        "name": user_record.get("name") or user_record["user_id"],
        "email": user_record.get("email"),
        "role": user_record.get("role", "Member"),
        "team": user_record.get("department", ""),
        "primary_goal": {
            "name": "—",
            "priority": "exploration",
            "timeline": "Not set",
            "days_left": None,
            "readiness": 0,
            "readiness_delta": "no goal yet",
        },
        "status": "exploring",
        "status_label": "Just joined",
        "last_active": user_record.get("last_active_at"),
        "sessions_30d": 0,
        "minutes_30d": 0,
        "completed_steps_30d": 0,
        "gaps_count": 0,
        "current_step": "Not started",
        "recent_sessions": [],
        "gaps": [],
        "kudos_count": 0,
    }


def _build_report_card(user_record: dict) -> dict:
    """Merge rbac user record with demo profile cache (or default)."""
    cached = _DEMO_PROFILE_CACHE.get(user_record["user_id"])
    if cached:
        # Refresh top-level identity fields from rbac (in case admin updated them)
        return {
            **cached,
            "name": user_record.get("name") or cached["name"],
            "email": user_record.get("email") or cached.get("email"),
            "role": user_record.get("role", cached.get("role")),
            "team": user_record.get("department") or cached.get("team", ""),
        }
    return _default_profile(user_record)


def list_team(manager_id: str, include_skip: bool = False) -> dict:
    """
    List the manager's direct reports — pulled from the org graph in rbac
    (manager_user_id field). Rich profiles come from the demo cache for
    the 5 canned personas; default profiles for anyone else.

    When include_skip=True, also includes skip-level reports (reports of
    your reports) — used by skip_manager role.
    """
    direct_records = rbac.get_reports(manager_id)
    skip_records = rbac.get_skip_reports(manager_id) if include_skip else []

    # Build report cards
    direct = [_build_report_card(r) for r in direct_records]
    skip = [{**_build_report_card(r), "is_skip": True, "via_manager_id": r.get("manager_user_id")} for r in skip_records]

    team = direct + skip

    summary = {
        "on_track":   sum(1 for m in team if m["status"] == "on_track"),
        "behind":     sum(1 for m in team if m["status"] == "behind"),
        "blocked":    sum(1 for m in team if m["status"] == "blocked"),
        "exploring":  sum(1 for m in team if m["status"] == "exploring"),
        "mandatory":  sum(1 for m in team if m["status"] == "mandatory"),
        "needs_attention": sum(1 for m in team if m.get("manager_attention_flag")),
        "avg_readiness": round(sum(m["primary_goal"]["readiness"] for m in team) / max(len(team), 1)) if team else 0,
        "total_sessions_30d": sum(m["sessions_30d"] for m in team),
        "total_minutes_30d":  sum(m["minutes_30d"] for m in team),
        "direct_count": len(direct),
        "skip_count":   len(skip),
    }

    # Aggregate goal coverage — what is the team learning?
    goals_by_priority = {}
    departments = {}
    for m in team:
        p = m["primary_goal"]["priority"]
        goals_by_priority[p] = goals_by_priority.get(p, 0) + 1
        d = m.get("team") or "Unassigned"
        departments[d] = departments.get(d, 0) + 1

    return {
        "manager_id": manager_id,
        "team": team,
        "count": len(team),
        "summary": summary,
        "goals_by_priority": goals_by_priority,
        "departments": departments,
        "include_skip": include_skip,
    }


def get_org_chart(root_user_id: str = None) -> dict:
    """Manager → reports tree starting from a root user (or every top-level user)."""
    return rbac.get_org_tree(root_user_id=root_user_id)


def get_team_member(manager_id: str, member_id: str) -> dict:
    """Detailed view of one report — same data as list_team's row, surfaced standalone."""
    team = list_team(manager_id, include_skip=True).get("team", [])
    member = next((m for m in team if m["user_id"] == member_id), None)
    if not member:
        return {"error": f"team member {member_id} not found"}
    member_kudos = [k for k in _KUDOS_LOG if k["report_id"] == member_id]
    return {**member, "kudos_received": member_kudos}


def send_kudos(manager_id: str, report_id: str, message: str = "") -> dict:
    """
    Manager sends kudos to a report. Records in _KUDOS_LOG and emits a feed
    event to the report's email so it shows in their right-rail activity.
    """
    if not report_id:
        return {"error": "report_id required"}
    team = list_team(manager_id, include_skip=True).get("team", [])
    member = next((m for m in team if m["user_id"] == report_id), None)
    if not member:
        return {"error": f"report {report_id} not in team"}

    record = {
        "kudos_id": f"k-{int(datetime.utcnow().timestamp())}",
        "manager_id": manager_id,
        "report_id": report_id,
        "report_email": member.get("email"),
        "report_name": member.get("name"),
        "message": message or "Great work this week 👏",
        "sent_at": datetime.utcnow().isoformat(),
    }
    _KUDOS_LOG.append(record)

    # Emit into the resume feed so the report sees it in their right rail.
    # Lazy import — feed lives in the resume module's social layer.
    try:
        from . import resume as _resume
        _resume._emit_feed(member.get("email") or "", {
            "type": "kudos_received",
            "from_manager_id": manager_id,
            "from_manager_name": "Your manager",  # Phase D: real name from auth
            "message": record["message"],
        })
    except Exception:
        pass

    return {"ok": True, "kudos": record}


def list_kudos_sent(manager_id: str) -> dict:
    """Manager-side log of kudos they've sent."""
    return {
        "manager_id": manager_id,
        "kudos": [k for k in _KUDOS_LOG if k["manager_id"] == manager_id],
        "count": sum(1 for k in _KUDOS_LOG if k["manager_id"] == manager_id),
    }
