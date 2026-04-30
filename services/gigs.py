"""
Gigs marketplace — Internal Pilot Pack · Phase F.5.

Intra-enterprise cross-team marketplace for short-form work:
   POST → CLAIM → DELIVER → REVIEW → POINTS → SERVICE RECORD

Closes the loop on Problem #12 from the sizing doc — "hands-on experiences
are invisible." Now they're posted, claimed, completed, validated, and
auto-attached to the doer's Resume / Service Record.

DESIGN
──────
- **Posting**: anyone with `learner` role+ can post. Learner posts go to
  `pending_approval` (manager must accept); manager-and-up posts go
  straight to `open`. (Approval flow is V2 — V1 auto-approves but flags.)
- **Claiming**: any active user can claim an `open` gig. One claim per
  gig. Manager of the claimer is notified (audit + feed event).
- **Delivering**: claimer submits a deliverable_url + notes, gig moves to
  `delivered`. Poster reviews.
- **Reviewing**: poster accepts (gig → `accepted` + `completed`, points
  awarded, Resume entry auto-created with poster as endorser) or
  declines (gig → `declined`, no points, claimer can re-deliver).
- **Points**: structured scale (25 / 50 / 100 / 200 / 500) chosen by
  poster at posting time. Phase V2 will tie to an awards catalog
  (gift cards, PTO hours, charity donations); V1 just tracks balance.

INTEGRATION POINTS
──────────────────
- Resume: `accept_delivery` calls `resume.add_entry(...)` with structured
  fields (title, description, outcomes, technologies, transferable_skills)
  PLUS an auto-endorsement from the poster (status='approved'). The
  endorsement loop with Resume is what makes a completed gig a
  resume-grade credential, not a vibe.
- Audit log: post / claim / deliver / accept / decline all hooked.
- Stay Ahead: not wired here; the Stay Ahead canvas can call /gigs/list
  with skill filter to surface "matching open gigs" alongside
  up-the-stack-moves.

PHASE 2 STORAGE
───────────────
In-memory dicts. Phase 2: Airtable Tables 28 (Gigs) + 29 (Points_Ledger).
"""

from datetime import datetime, timedelta


VALID_STATUSES = {
    "draft", "pending_approval", "open", "claimed",
    "in_progress", "delivered", "accepted", "completed",
    "declined", "cancelled", "expired",
}

# Structured point scale
POINT_TIERS = {
    25:  {"hours": "≤1",     "label": "Quick task"},
    50:  {"hours": "2–4",    "label": "Half-day"},
    100: {"hours": "5–10",   "label": "Full day"},
    200: {"hours": "10–20",  "label": "Multi-day"},
    500: {"hours": "20+",    "label": "Project"},
}
VALID_POINT_VALUES = set(POINT_TIERS.keys())


# ──────────────────────────────────────────────────────────────
# In-memory storage. Phase 2: Airtable Tables 28 + 29.
# ──────────────────────────────────────────────────────────────

_GIGS = {}          # gig_id → gig dict
_POINTS_LEDGER = []  # list of {user_id, amount, gig_id, reason, ts}
_GIG_SEQ = [0]


def _next_id():
    _GIG_SEQ[0] += 1
    return _GIG_SEQ[0]


# ──────────────────────────────────────────────────────────────
# DEMO SEED — a few open gigs so the marketplace renders populated
# ──────────────────────────────────────────────────────────────

def _seed_demo_gigs():
    if _GIGS:
        return  # already seeded
    today = datetime.utcnow()
    seeds = [
        {
            "title": "Refactor the deploy pipeline's Slack notifications",
            "description": "Our deploy pipeline spams the #engineering channel on every PR — needs to roll up to one notification per release with structured fields. Should be a 4-6 hour script change.",
            "skills": ["python", "docker"],
            "department_origin": "Platform Engineering",
            "point_value": 50,
            "estimated_hours": "3-4",
            "deadline_at": (today + timedelta(days=7)).isoformat(),
            "posted_by": "demo-user",
            "posted_by_name": "Sarah Chen (demo)",
            "status": "open",
        },
        {
            "title": "Internal lunch-and-learn on Service Mesh — 30 min",
            "description": "Platform team is rolling out Istio next quarter. Need someone with hands-on Service Mesh experience to give a 30-min internal session for the broader engineering org. ~5 hours of prep + delivery.",
            "skills": ["kubernetes", "networking"],
            "department_origin": "People & Learning",
            "point_value": 100,
            "estimated_hours": "5-6",
            "deadline_at": (today + timedelta(days=14)).isoformat(),
            "posted_by": "raj-kumar",
            "posted_by_name": "Raj Kumar",
            "status": "open",
        },
        {
            "title": "Audit our AWS IAM policies for least-privilege violations",
            "description": "Quick security audit — pull all our IAM roles, flag any with `*` resource access, write up findings. ~6-8 hours of focused work.",
            "skills": ["aws", "security"],
            "department_origin": "Legal & Compliance",
            "point_value": 100,
            "estimated_hours": "6-8",
            "deadline_at": (today + timedelta(days=21)).isoformat(),
            "posted_by": "legal-contact",
            "posted_by_name": "Legal Contact",
            "status": "open",
        },
        {
            "title": "Mentor a new engineer joining the data team",
            "description": "Looking for an experienced engineer to be a bi-weekly mentor for our incoming Data Engineer (starts Monday). 2 sessions/month for 3 months. Structured kickoff + open Q&A format.",
            "skills": ["leadership", "mlops"],
            "department_origin": "Platform Engineering",
            "point_value": 200,
            "estimated_hours": "12-15",
            "deadline_at": (today + timedelta(days=90)).isoformat(),
            "posted_by": "demo-user",
            "posted_by_name": "Sarah Chen (demo)",
            "status": "open",
        },
        {
            "title": "Write a runbook for our DB failover procedure",
            "description": "Currently undocumented — when prod DB goes read-only, who knows what to do? Need a step-by-step runbook with screenshots and rollback procedures. 4-6 hours.",
            "skills": ["documentation"],
            "department_origin": "Platform Engineering",
            "point_value": 50,
            "estimated_hours": "4-6",
            "deadline_at": (today + timedelta(days=10)).isoformat(),
            "posted_by": "demo-user",
            "posted_by_name": "Sarah Chen (demo)",
            "status": "open",
        },
    ]
    for seed in seeds:
        gig_id = _next_id()
        _GIGS[gig_id] = {
            "gig_id": gig_id,
            "created_at": today.isoformat(),
            "updated_at": today.isoformat(),
            "claimed_by": None,
            "claimed_by_name": None,
            "claimed_at": None,
            "delivered_at": None,
            "deliverable_url": None,
            "deliverable_notes": None,
            "reviewed_at": None,
            "review_notes": None,
            "completed_at": None,
            "auto_resume_entry_id": None,
            **seed,
        }


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────

def post_gig(actor_user_id: str, profile: dict) -> dict:
    """
    Post a new gig. Required: title, description, skills, point_value.
    Optional: estimated_hours, deadline_at, department_origin.

    learner role → status = 'pending_approval' (admin/manager must approve)
    manager+ → status = 'open' immediately
    """
    if not actor_user_id:
        return {"error": "actor_user_id required"}
    if not profile.get("title"):
        return {"error": "title required"}
    if not profile.get("description"):
        return {"error": "description required"}
    if not profile.get("skills"):
        return {"error": "skills required (at least one)"}
    point_value = int(profile.get("point_value") or 0)
    if point_value not in VALID_POINT_VALUES:
        return {"error": f"point_value must be one of {sorted(VALID_POINT_VALUES)}"}

    # Determine initial status from poster's role
    from . import rbac as _rbac
    role = _rbac.get_role(actor_user_id)
    starter_status = "open" if role in ("manager", "skip_manager", "ld_admin", "compliance_admin", "org_admin", "super_admin") else "pending_approval"

    poster = _rbac.get_user(actor_user_id)
    gig_id = _next_id()
    now = datetime.utcnow().isoformat()
    gig = {
        "gig_id": gig_id,
        "title": profile["title"].strip(),
        "description": profile["description"].strip(),
        "skills": profile.get("skills") or [],
        "department_origin": profile.get("department_origin") or poster.get("department") or "",
        "point_value": point_value,
        "estimated_hours": profile.get("estimated_hours", ""),
        "deadline_at": profile.get("deadline_at"),
        "posted_by": actor_user_id,
        "posted_by_name": poster.get("name") or actor_user_id,
        "status": starter_status,
        "claimed_by": None, "claimed_by_name": None, "claimed_at": None,
        "delivered_at": None, "deliverable_url": None, "deliverable_notes": None,
        "reviewed_at": None, "review_notes": None,
        "completed_at": None,
        "auto_resume_entry_id": None,
        "created_at": now, "updated_at": now,
    }
    _GIGS[gig_id] = gig
    return {"ok": True, "gig": gig, "auto_published": starter_status == "open"}


def list_gigs(status: str = None, skill: str = None, department: str = None,
              search: str = None, limit: int = 50) -> dict:
    """Browse gigs. Defaults: open gigs only, sorted newest first."""
    _seed_demo_gigs()
    gigs = list(_GIGS.values())

    if status:
        gigs = [g for g in gigs if g["status"] == status]
    elif status is None:
        # Default to "browseable" statuses (open + claimed)
        gigs = [g for g in gigs if g["status"] in ("open", "claimed", "in_progress")]

    if skill:
        s = skill.lower().strip()
        gigs = [g for g in gigs if s in [k.lower() for k in (g.get("skills") or [])]]
    if department:
        gigs = [g for g in gigs if (g.get("department_origin") or "") == department]
    if search:
        q = search.lower()
        gigs = [
            g for g in gigs
            if q in (g.get("title") or "").lower()
            or q in (g.get("description") or "").lower()
            or any(q in s.lower() for s in (g.get("skills") or []))
        ]

    gigs.sort(key=lambda g: g.get("created_at", ""), reverse=True)
    gigs = gigs[:limit]

    # Aggregate stats
    by_skill = {}
    by_dept = {}
    by_point = {}
    open_total_points = 0
    for g in _GIGS.values():
        if g["status"] != "open":
            continue
        for sk in (g.get("skills") or []):
            by_skill[sk] = by_skill.get(sk, 0) + 1
        d = g.get("department_origin") or "Unassigned"
        by_dept[d] = by_dept.get(d, 0) + 1
        by_point[g["point_value"]] = by_point.get(g["point_value"], 0) + 1
        open_total_points += g["point_value"]

    return {
        "gigs": gigs,
        "count": len(gigs),
        "summary": {
            "open_count": sum(1 for g in _GIGS.values() if g["status"] == "open"),
            "claimed_count": sum(1 for g in _GIGS.values() if g["status"] in ("claimed", "in_progress", "delivered")),
            "completed_count": sum(1 for g in _GIGS.values() if g["status"] == "completed"),
            "by_skill": dict(sorted(by_skill.items(), key=lambda kv: -kv[1])[:12]),
            "by_department": by_dept,
            "by_point_value": by_point,
            "total_open_points": open_total_points,
        },
    }


def get_gig(gig_id: int) -> dict:
    _seed_demo_gigs()
    g = _GIGS.get(int(gig_id) if str(gig_id).isdigit() else gig_id)
    return g or {"error": f"gig {gig_id} not found"}


def claim_gig(actor_user_id: str, gig_id: int) -> dict:
    """Claim an open gig. One claim per gig."""
    g = _GIGS.get(int(gig_id) if str(gig_id).isdigit() else gig_id)
    if not g:
        return {"error": f"gig {gig_id} not found"}
    if g["status"] != "open":
        return {"error": f"gig is not open (current: {g['status']})"}
    if g["posted_by"] == actor_user_id:
        return {"error": "you posted this gig — you can't claim your own"}
    from . import rbac as _rbac
    claimer = _rbac.get_user(actor_user_id)
    g.update({
        "status": "claimed",
        "claimed_by": actor_user_id,
        "claimed_by_name": claimer.get("name") or actor_user_id,
        "claimed_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    })
    return {"ok": True, "gig": g}


def deliver_gig(actor_user_id: str, gig_id: int, deliverable_url: str = "", notes: str = "") -> dict:
    """Claimer marks delivery complete. Poster reviews next."""
    g = _GIGS.get(int(gig_id) if str(gig_id).isdigit() else gig_id)
    if not g:
        return {"error": f"gig {gig_id} not found"}
    if g["status"] not in ("claimed", "in_progress", "declined"):
        return {"error": f"gig is not in deliverable state (current: {g['status']})"}
    if g["claimed_by"] != actor_user_id:
        return {"error": "only the claimer can mark this delivered"}
    g.update({
        "status": "delivered",
        "delivered_at": datetime.utcnow().isoformat(),
        "deliverable_url": deliverable_url.strip(),
        "deliverable_notes": notes.strip(),
        "updated_at": datetime.utcnow().isoformat(),
    })
    return {"ok": True, "gig": g}


def review_gig(actor_user_id: str, gig_id: int, decision: str, review_notes: str = "") -> dict:
    """
    Poster accepts or declines a delivered gig.

    On accept:
      - status → completed
      - points awarded to claimer
      - Resume entry auto-created with poster auto-endorsing the work
      - Audit hook fires
    """
    g = _GIGS.get(int(gig_id) if str(gig_id).isdigit() else gig_id)
    if not g:
        return {"error": f"gig {gig_id} not found"}
    if g["status"] != "delivered":
        return {"error": f"gig is not awaiting review (current: {g['status']})"}
    if g["posted_by"] != actor_user_id:
        return {"error": "only the poster can review this delivery"}
    if decision not in ("accept", "decline"):
        return {"error": "decision must be 'accept' or 'decline'"}

    now = datetime.utcnow().isoformat()
    g.update({
        "status": "completed" if decision == "accept" else "declined",
        "reviewed_at": now,
        "review_notes": review_notes.strip(),
        "updated_at": now,
    })

    if decision == "accept":
        g["completed_at"] = now
        # Award points
        _POINTS_LEDGER.append({
            "user_id": g["claimed_by"],
            "amount": g["point_value"],
            "gig_id": g["gig_id"],
            "reason": f"Completed gig: {g['title']}",
            "ts": now,
        })
        # Auto-create Resume entry with poster as auto-endorser
        try:
            from . import resume as _resume
            structured = {
                "title": g["title"],
                "category": "project",
                "description": g.get("description", ""),
                "outcomes": [
                    f"Delivered via Aasan Gigs ({g['point_value']} points)",
                    g.get("deliverable_notes") or "",
                ],
                "technologies": list(g.get("skills") or []),
                "stakeholders": [g["posted_by_name"]],
                "transferable_skills": list(g.get("skills") or []),
                "company": "",
                "project": f"Aasan Gigs · {g.get('department_origin', '')}".strip(" ·"),
            }
            entry_result = _resume.add_entry(
                user_id=g["claimed_by"],
                raw_input=g.get("description", ""),
                structured=structured,
            )
            entry_id = (entry_result or {}).get("entry", {}).get("entry_id")
            g["auto_resume_entry_id"] = entry_id

            # Auto-endorse the entry (poster vouches that the work was completed)
            from . import rbac as _rbac
            poster = _rbac.get_user(g["posted_by"])
            _resume.endorse_entry(
                author_user_id=g["claimed_by"],
                entry_id=entry_id,
                endorser_email=poster.get("email") or g["posted_by"],
                endorser_name=g["posted_by_name"],
                endorser_role=poster.get("role", ""),
                comment=review_notes or f"Completed the {g['title']} gig successfully.",
            )

            # Feed event for the claimer
            claimer = _rbac.get_user(g["claimed_by"])
            _resume._emit_feed(claimer.get("email") or g["claimed_by"], {
                "type": "gig_accepted",
                "gig_id": g["gig_id"],
                "gig_title": g["title"],
                "points_awarded": g["point_value"],
                "from_user_id": g["posted_by"],
                "from_user_name": g["posted_by_name"],
                "comment": review_notes,
            })
        except Exception as exc:
            print(f"[gigs] auto-resume on accept failed: {exc}")

    return {"ok": True, "gig": g}


def cancel_gig(actor_user_id: str, gig_id: int, reason: str = "") -> dict:
    """Poster cancels their own gig (or org_admin overrides)."""
    g = _GIGS.get(int(gig_id) if str(gig_id).isdigit() else gig_id)
    if not g:
        return {"error": f"gig {gig_id} not found"}
    from . import rbac as _rbac
    if g["posted_by"] != actor_user_id and not _rbac.has_any_permission(actor_user_id, "admin:users"):
        return {"error": "only the poster or org_admin can cancel this gig"}
    if g["status"] in ("completed", "cancelled"):
        return {"error": f"already {g['status']}"}
    g.update({
        "status": "cancelled",
        "review_notes": reason.strip() or g.get("review_notes", ""),
        "updated_at": datetime.utcnow().isoformat(),
    })
    return {"ok": True, "gig": g}


def list_my_posts(user_id: str) -> dict:
    return {
        "user_id": user_id,
        "gigs": [g for g in _GIGS.values() if g["posted_by"] == user_id],
    }


def list_my_claims(user_id: str) -> dict:
    return {
        "user_id": user_id,
        "gigs": [g for g in _GIGS.values() if g.get("claimed_by") == user_id],
    }


def get_points(user_id: str) -> dict:
    entries = [e for e in _POINTS_LEDGER if e["user_id"] == user_id]
    balance = sum(e["amount"] for e in entries)
    return {
        "user_id": user_id,
        "balance": balance,
        "history": sorted(entries, key=lambda e: e["ts"], reverse=True),
        "earned_count": len(entries),
    }


def points_leaderboard(limit: int = 10) -> dict:
    """Org-wide leaderboard. Optional admin / motivational surface."""
    by_user = {}
    for e in _POINTS_LEDGER:
        by_user[e["user_id"]] = by_user.get(e["user_id"], 0) + e["amount"]
    rows = sorted(by_user.items(), key=lambda kv: -kv[1])[:limit]
    return {"leaderboard": [{"user_id": u, "points": p} for u, p in rows]}
