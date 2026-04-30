"""
Skill heatmap — Internal Pilot Pack · Phase E.

The L&D buyer's headline view: a 2D matrix of `department × skill_cluster`
showing where the org is investing learning — and where it isn't.

The single most valuable analytical output:
  identified GAPS = (skill, department) cells where DEMAND
  (people goal-targeting / actively learning) is HIGH but SUPPLY
  (content indexed + SMEs available) is LOW.

DESIGN
──────
- **Skill clusters** come from `services/content_classifier.SKILL_VOCAB`
  (kubernetes / aws / gcp / docker / terraform / python / javascript /
  data-modeling / sql / ml / mlops / security / networking /
  observability / leadership). Plus dynamically-added clusters from
  classified content_index entries that introduced new clusters.
- **Demand signal per (dept, skill):**
    1. Users in `dept` whose primary_goal name or step titles contain a
       keyword from the cluster's vocabulary.
    2. Recent session titles from cached profiles in team module.
- **Supply signal per skill (org-wide):**
    1. Content items in `content_index` with that skill in `skills[]`.
    2. SMEs in `sme.REGISTERED_SMES + INTERNAL_SMES + EXTERNAL_SMES`
       offering that topic.
- **Gap detection:** demand_count >= 2 AND supply_count == 0 → "blind spot."

INTERFACE
─────────
  build_heatmap(departments_filter=None) -> dict
"""

from datetime import datetime

from . import rbac, team as team_svc, path_engine, content_classifier, sme
from .content_classifier import SKILL_VOCAB


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _all_active_users():
    rbac._ensure_user("demo-user")  # trigger seed
    return [u for u in rbac._USERS.values() if u.get("is_active", True)]


def _content_index():
    """Lazy import of the in-memory content_index from app.py to avoid circular."""
    try:
        from app import content_index as _ci
        return _ci
    except Exception:
        return []


def _vocab_match(text, vocabulary_kws):
    """Case-insensitive substring match against a list of keywords."""
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in vocabulary_kws)


def _user_skill_signals(user):
    """
    For a user, return the set of skill_cluster names whose vocabulary
    matches the user's primary goal name + path step titles + recent
    session titles.
    """
    matched = set()
    profile = team_svc._build_report_card(user)
    blob_parts = []

    if profile:
        blob_parts.append((profile.get("primary_goal") or {}).get("name", ""))
        blob_parts.append(profile.get("current_step", ""))
        for s in (profile.get("recent_sessions") or []):
            blob_parts.append(s.get("title", ""))

    # Path engine — goals + step titles for this user
    user_data = path_engine._STORE.get(user["user_id"]) or {}
    for goal_id, entry in user_data.items():
        goal = entry.get("goal") or {}
        blob_parts.append(goal.get("name", ""))
        for step in (entry.get("path") or {}).get("steps", []) or []:
            blob_parts.append(step.get("title", ""))

    blob = " ".join([p for p in blob_parts if p]).lower()

    for cluster, kws in SKILL_VOCAB.items():
        if _vocab_match(blob, [k.lower() for k in kws]):
            matched.add(cluster)
    return matched


# ──────────────────────────────────────────────────────────────
# Public — build_heatmap
# ──────────────────────────────────────────────────────────────

def build_heatmap(departments_filter: list = None) -> dict:
    """
    Returns:
      {
        skill_clusters: [str],        # column order, sorted by total demand desc
        departments:    [str],        # row order
        matrix:         [[int]],      # [dept_idx][skill_idx] = people count
        cell_users:     {dept: {skill: [{user_id, name, role, status}]}},
        supply: {
          content: {skill: count},
          smes:    {skill: count},
        },
        demand_total:   {skill: count},
        gaps: [{skill, departments, demand, supply, severity}],
        summary: {...},
        generated_at,
      }
    """
    users = _all_active_users()
    if departments_filter:
        users = [u for u in users if (u.get("department") or "Unassigned") in departments_filter]

    # Compute per-user matched clusters
    per_user = []
    for u in users:
        matched = _user_skill_signals(u)
        if matched:
            per_user.append((u, matched))

    # Aggregate (department, skill) → users
    cell_users = {}
    demand_total = {}
    departments_set = set()

    for u, skills in per_user:
        dept = u.get("department") or "Unassigned"
        departments_set.add(dept)
        cell_users.setdefault(dept, {})
        for s in skills:
            cell_users[dept].setdefault(s, []).append({
                "user_id": u["user_id"],
                "name": u.get("name") or u["user_id"],
                "role": u.get("role"),
                "status": (team_svc._build_report_card(u) or {}).get("status", "exploring"),
            })
            demand_total[s] = demand_total.get(s, 0) + 1

    # Always include the user's department even if they have no skills (for completeness)
    for u in users:
        departments_set.add(u.get("department") or "Unassigned")

    # SUPPLY — content + SMEs per skill
    content_supply = {}
    for c in _content_index():
        for s in (c.get("skills") or []):
            s = s.strip().lower() if isinstance(s, str) else s
            content_supply[s] = content_supply.get(s, 0) + 1

    sme_supply = {}
    sme_pool = (
        getattr(sme, "REGISTERED_SMES", []) or []
    ) + sme.INTERNAL_SMES + sme.EXTERNAL_SMES
    for s in sme_pool:
        for topic in (s.get("topics") or []):
            cluster = _topic_to_cluster(topic)
            if cluster:
                sme_supply[cluster] = sme_supply.get(cluster, 0) + 1

    # Order columns: skills with any demand or supply, sorted by total demand
    all_skills = set(demand_total.keys()) | set(content_supply.keys()) | set(sme_supply.keys())
    skill_clusters = sorted(
        all_skills,
        key=lambda s: (-(demand_total.get(s, 0)), -(content_supply.get(s, 0)), s),
    )

    # Order rows: by total people count desc
    departments = sorted(
        departments_set,
        key=lambda d: -sum(len(cell_users.get(d, {}).get(s, [])) for s in skill_clusters),
    )

    matrix = [
        [len(cell_users.get(d, {}).get(s, [])) for s in skill_clusters]
        for d in departments
    ]

    # Gaps: demand >= 2 AND content + sme supply == 0
    gaps = []
    for s in skill_clusters:
        d_count = demand_total.get(s, 0)
        supply = content_supply.get(s, 0) + sme_supply.get(s, 0)
        if d_count >= 2 and supply == 0:
            depts_in_gap = [d for d in departments if cell_users.get(d, {}).get(s)]
            gaps.append({
                "skill": s,
                "departments": depts_in_gap,
                "demand": d_count,
                "supply": supply,
                "severity": "high" if d_count >= 3 else "medium",
            })
    # also low-supply (some content, but ratio < 1 content per 3 demanders)
    for s in skill_clusters:
        d_count = demand_total.get(s, 0)
        c_count = content_supply.get(s, 0)
        if d_count >= 3 and c_count > 0 and c_count < d_count / 3:
            gaps.append({
                "skill": s,
                "departments": [d for d in departments if cell_users.get(d, {}).get(s)],
                "demand": d_count,
                "supply": c_count,
                "severity": "medium",
            })

    return {
        "skill_clusters": skill_clusters,
        "departments": departments,
        "matrix": matrix,
        "cell_users": cell_users,
        "supply": {
            "content": {k: content_supply.get(k, 0) for k in skill_clusters},
            "smes":    {k: sme_supply.get(k, 0) for k in skill_clusters},
        },
        "demand_total": {k: demand_total.get(k, 0) for k in skill_clusters},
        "gaps": gaps,
        "summary": {
            "users_in_scope": len(users),
            "users_with_skill_signal": len(per_user),
            "departments": len(departments),
            "skill_clusters_tracked": len(skill_clusters),
            "total_demand": sum(demand_total.values()),
            "total_content_items": sum(content_supply.values()),
            "total_smes_offering": sum(sme_supply.values()),
            "gaps_detected": len(gaps),
        },
        "generated_at": datetime.utcnow().isoformat(),
    }


def _topic_to_cluster(topic: str) -> str:
    """Map a freeform SME topic to a SKILL_VOCAB cluster (or empty if no match)."""
    if not topic:
        return ""
    t = topic.lower()
    for cluster, kws in SKILL_VOCAB.items():
        for kw in kws:
            if kw.lower() in t:
                return cluster
    return ""
