"""
SME Marketplace V1.

Two surfaces, one matcher:

  1. Internal SME directory — employees who've opted in to help on specific
     topics. Auto-derived candidates from the knowledge graph (high mastery
     + recent activity), filtered to those who opted in.

  2. External SME network (curated, hand-picked V1) — vetted external experts
     with rate, availability, language, rating.

PHASE 1 STORAGE
───────────────
Hardcoded demo set below. Phase 2:
  - Internal: Cypher over Neo4j Concept nodes joined to opted-in Employee nodes
  - External: Airtable SME_Profiles table (Table 19)
  - Bookings: Airtable SME_Bookings (20) + SME_Sessions (21)

WHEN PERAASAN ESCALATES TO AN SME
─────────────────────────────────
The Tutor Mode struggle-detector fires after the learner asks 3+ clarifying
questions on the same concept. Peraasan offers SME options inline. The
matcher ranks candidates by (mastery × recency × opt-in × availability).
"""

import re
from datetime import datetime, timedelta, timezone as tz


# ──────────────────────────────────────────────────────────────
# Internal SME directory — demo seed
# Phase 2: auto-derived from Neo4j knowledge graph
# ──────────────────────────────────────────────────────────────

INTERNAL_SMES = [
    {
        "sme_id": "internal-1",
        "sme_type": "internal",
        "name": "David Kim",
        "role": "Senior Site Reliability Engineer",
        "team": "Platform Engineering",
        "topics": ["Service Mesh", "Kubernetes networking", "Istio", "Observability", "Multi-cluster K8s"],
        "topic_mastery": {
            "Service Mesh": 0.87,
            "Kubernetes networking": 0.92,
            "Istio": 0.85,
            "Observability": 0.78,
            "Multi-cluster K8s": 0.82,
        },
        "recent_activity_days_ago": 2,
        "availability_window": "Tue/Thu PM, 30-min slots",
        "timezone": "America/Los_Angeles",
        "rate_per_30min": 0,  # internal — kudos-only
        "rate_currency": None,
        "languages": ["en"],
        "sessions_completed": 12,
        "kudos_score": 4.9,
        "opted_in": True,
        "next_available": "Thu 14:00 PT",
    },
    {
        "sme_id": "internal-2",
        "sme_type": "internal",
        "name": "Emily Mendez",
        "role": "Staff Platform Engineer",
        "team": "Platform Engineering",
        "topics": ["Service Mesh", "Cloud security", "IAM", "Zero-trust"],
        "topic_mastery": {
            "Service Mesh": 0.82,
            "Cloud security": 0.88,
            "IAM": 0.85,
        },
        "recent_activity_days_ago": 7,
        "availability_window": "Mon/Wed AM, 30-min slots",
        "timezone": "America/New_York",
        "rate_per_30min": 0,
        "rate_currency": None,
        "languages": ["en", "es"],
        "sessions_completed": 6,
        "kudos_score": 4.8,
        "opted_in": True,
        "next_available": "Mon 10:00 ET",
    },
    {
        "sme_id": "internal-3",
        "sme_type": "internal",
        "name": "Aisha Singh",
        "role": "Cloud Architect",
        "team": "Architecture",
        "topics": ["Service Mesh", "Multi-region design", "AWS architecture", "FinOps"],
        "topic_mastery": {
            "Service Mesh": 0.78,
            "Multi-region design": 0.95,
            "FinOps": 0.81,
        },
        "recent_activity_days_ago": 21,  # On parental leave
        "availability_window": "On parental leave until June 2026",
        "timezone": "America/Los_Angeles",
        "rate_per_30min": 0,
        "rate_currency": None,
        "languages": ["en", "hi", "pa"],
        "sessions_completed": 23,
        "kudos_score": 5.0,
        "opted_in": False,  # Not currently available
        "next_available": "Returns June 2026",
    },
]


# ──────────────────────────────────────────────────────────────
# External SME marketplace — hand-picked V1 (5 external experts)
# Phase 2: open marketplace with vetting + reviews
# ──────────────────────────────────────────────────────────────

EXTERNAL_SMES = [
    {
        "sme_id": "external-1",
        "sme_type": "external",
        "name": "Maya Patel",
        "role": "Independent Service Mesh Consultant",
        "topics": ["Service Mesh", "Istio", "Linkerd", "mTLS"],
        "topic_mastery": {"Service Mesh": 0.95, "Istio": 0.93, "mTLS": 0.92},
        "availability_window": "Most weekdays 9-5 PT, 30-min slots",
        "timezone": "America/Los_Angeles",
        "rate_per_30min": 40,
        "rate_currency": "usd",
        "languages": ["en", "hi"],
        "sessions_completed": 87,
        "kudos_score": 4.9,
        "opted_in": True,
        "next_available": "in ~90 min",
        "bio": "Former Google SRE, now independent consultant. 7 years deep in Service Mesh.",
    },
    {
        "sme_id": "external-2",
        "sme_type": "external",
        "name": "Carlos Rivera",
        "role": "Kubernetes & Istio Expert",
        "topics": ["Kubernetes", "Istio", "Service Mesh", "GitOps"],
        "topic_mastery": {"Service Mesh": 0.88, "Kubernetes": 0.92, "GitOps": 0.85},
        "availability_window": "Tue/Wed/Thu, 30-min or 60-min slots",
        "timezone": "America/Mexico_City",
        "rate_per_30min": 60,
        "rate_currency": "usd",
        "languages": ["en", "es"],
        "sessions_completed": 134,
        "kudos_score": 4.8,
        "opted_in": True,
        "next_available": "tomorrow 10:00 CT",
        "bio": "CKA + CKAD certified trainer. Helps engineers ramp on K8s + Istio at startups.",
    },
    {
        "sme_id": "external-3",
        "sme_type": "external",
        "name": "Alex Park",
        "role": "K8s contributor (free / kudos-only)",
        "topics": ["Kubernetes", "Service Mesh", "Open source"],
        "topic_mastery": {"Service Mesh": 0.82, "Kubernetes": 0.94},
        "availability_window": "Weekends, occasional weekday evenings",
        "timezone": "Europe/London",
        "rate_per_30min": 0,
        "rate_currency": None,
        "languages": ["en", "ko"],
        "sessions_completed": 28,
        "kudos_score": 4.7,
        "opted_in": True,
        "next_available": "next Saturday 10:00 BST",
        "bio": "K8s SIG-network contributor. Helps for kudos — pay it forward when you're ready.",
    },
    {
        "sme_id": "external-4",
        "sme_type": "external",
        "name": "Priya Iyer",
        "role": "Cloud Architecture Advisor",
        "topics": ["AWS", "Multi-region", "FinOps", "Cloud Architecture"],
        "topic_mastery": {"FinOps": 0.92, "Multi-region": 0.88, "AWS Architecture": 0.9},
        "availability_window": "Mon-Fri 9-6 IST",
        "timezone": "Asia/Kolkata",
        "rate_per_30min": 50,
        "rate_currency": "usd",
        "languages": ["en", "ta", "hi"],
        "sessions_completed": 156,
        "kudos_score": 4.95,
        "opted_in": True,
        "next_available": "today 19:00 IST",
        "bio": "Ex-AWS Solutions Architect. Specializes in multi-region cost optimization for SaaS companies.",
    },
]


# ──────────────────────────────────────────────────────────────
# In-memory bookings store (Phase 2: Airtable SME_Bookings)
# ──────────────────────────────────────────────────────────────

BOOKINGS = []


# ──────────────────────────────────────────────────────────────
# Self-registered SMEs — opt-in flow (Phase 2: Airtable SME_Profiles)
# Distinct from the demo seed INTERNAL_SMES/EXTERNAL_SMES so re-runs of
# the seed don't clobber real registrations. find_smes() walks both.
# ──────────────────────────────────────────────────────────────

REGISTERED_SMES = []


VALID_RATE_MODELS = {"free", "kudos_only", "paid"}
VALID_SESSION_LENGTHS = {15, 30, 45, 60}


def register_sme(employee_id: str, profile: dict) -> dict:
    """
    Self-registration for an SME (internal or external).

    Required fields in `profile`:
      - name (str)
      - subjects (list[str], 1+ topic clusters)

    Recommended fields:
      - role / team
      - subject_mastery (dict: {subject: 'beginner'|'intermediate'|'expert'|'can_teach'})
      - schedule_window (str — free-form "Tue/Thu PM, 30-min slots")
      - timezone (IANA, e.g. 'America/Los_Angeles' — auto-detected client-side)
      - languages (list[str], default ['en'])
      - rate_model ('free' | 'kudos_only' | 'paid')
      - rate_per_30min (number, required if rate_model == 'paid')
      - rate_currency (3-letter, required if rate_model == 'paid')
      - expectations_from_students (str — what should students DO before booking?)
      - bio (str)
      - preferred_session_length (15 | 30 | 45 | 60)
      - sme_type ('internal' | 'external', default 'internal')

    Idempotent on employee_id: re-registering updates the profile, doesn't dupe.
    """
    if not employee_id:
        return {"error": "employee_id required"}
    name = (profile.get("name") or "").strip()
    if not name:
        return {"error": "profile.name required"}
    subjects = [s.strip() for s in (profile.get("subjects") or []) if s and str(s).strip()]
    if not subjects:
        return {"error": "profile.subjects required (at least one)"}

    rate_model = profile.get("rate_model", "kudos_only")
    if rate_model not in VALID_RATE_MODELS:
        return {"error": f"rate_model must be one of {sorted(VALID_RATE_MODELS)}"}
    if rate_model == "paid":
        if profile.get("rate_per_30min") in (None, 0, ""):
            return {"error": "rate_per_30min required when rate_model='paid'"}
        if not (profile.get("rate_currency") or "").strip():
            return {"error": "rate_currency required when rate_model='paid'"}

    session_len = int(profile.get("preferred_session_length", 30))
    if session_len not in VALID_SESSION_LENGTHS:
        return {"error": f"preferred_session_length must be one of {sorted(VALID_SESSION_LENGTHS)}"}

    sme_type = profile.get("sme_type", "internal")

    # Build topic_mastery dict for the find_smes ranking pipeline
    declared = profile.get("subject_mastery") or {}
    mastery_map = {"beginner": 0.4, "intermediate": 0.65, "expert": 0.85, "can_teach": 0.9}
    topic_mastery = {
        s: mastery_map.get(declared.get(s, "expert"), 0.85)
        for s in subjects
    }

    now = datetime.utcnow().isoformat()
    sme_id = f"reg-{employee_id}"
    record = {
        "sme_id": sme_id,
        "sme_type": sme_type,
        "employee_id": employee_id,
        "name": name,
        "role": profile.get("role", ""),
        "team": profile.get("team", ""),
        "topics": subjects,
        "subject_mastery_declared": declared,
        "topic_mastery": topic_mastery,
        "schedule_window": profile.get("schedule_window", ""),
        "availability_window": profile.get("schedule_window", ""),  # alias for legacy seed shape
        "timezone": profile.get("timezone") or "UTC",
        "languages": profile.get("languages") or ["en"],
        "rate_model": rate_model,
        "rate_per_30min": float(profile.get("rate_per_30min") or 0),
        "rate_currency": (profile.get("rate_currency") or "").lower() or None,
        "expectations_from_students": profile.get("expectations_from_students", ""),
        "bio": profile.get("bio", ""),
        "preferred_session_length": session_len,
        "sessions_completed": 0,
        "kudos_score": 5.0,
        "opted_in": True,
        "registered_at": now,
        "updated_at": now,
        "recent_activity_days_ago": 0,
    }

    # Idempotent upsert
    for i, existing in enumerate(REGISTERED_SMES):
        if existing.get("employee_id") == employee_id:
            record["registered_at"] = existing.get("registered_at", now)
            record["sessions_completed"] = existing.get("sessions_completed", 0)
            record["kudos_score"] = existing.get("kudos_score", 5.0)
            REGISTERED_SMES[i] = record
            return {"ok": True, "created": False, "sme": record}
    REGISTERED_SMES.append(record)
    return {"ok": True, "created": True, "sme": record}


def get_sme_profile(employee_id: str) -> dict:
    """Return an SME's own profile (for the edit form)."""
    if not employee_id:
        return {"error": "employee_id required"}
    sme = next((s for s in REGISTERED_SMES if s.get("employee_id") == employee_id), None)
    if not sme:
        return {"registered": False, "employee_id": employee_id}
    return {"registered": True, "sme": sme}


def list_smes(active_only: bool = True, limit: int = 100) -> dict:
    """List all SMEs (registered + demo seed). Used by the SME Marketplace browse view."""
    all_smes = list(REGISTERED_SMES) + list(INTERNAL_SMES) + list(EXTERNAL_SMES)
    if active_only:
        all_smes = [s for s in all_smes if s.get("opted_in")]
    return {
        "smes": all_smes[:limit],
        "count": len(all_smes),
        "registered_count": len(REGISTERED_SMES),
        "demo_seed_count": len(INTERNAL_SMES) + len(EXTERNAL_SMES),
    }


def find_smes(topic: str, learner_id: str = None, limit: int = 5) -> dict:
    """
    Match SMEs against a topic. Returns ranked list with internal first
    (cheaper / more available), then external by relevance.

    Ranking signal: (topic_mastery × opted_in × availability_recency × kudos_score)
    """
    topic_lower = topic.lower()
    candidates = []

    for sme in REGISTERED_SMES + INTERNAL_SMES + EXTERNAL_SMES:
        # Match topic — substring or exact match
        match_mastery = 0.0
        for sme_topic, mastery in sme.get("topic_mastery", {}).items():
            if topic_lower in sme_topic.lower() or sme_topic.lower() in topic_lower:
                match_mastery = max(match_mastery, mastery)

        if match_mastery == 0:
            continue
        if not sme.get("opted_in"):
            continue

        # Score
        recency_factor = 1.0
        if sme["sme_type"] == "internal":
            days = sme.get("recent_activity_days_ago", 30)
            recency_factor = max(0.3, 1.0 - (days / 30))
        kudos = sme.get("kudos_score", 4.0) / 5.0
        score = match_mastery * recency_factor * kudos

        # Internal slight boost (preference for teammates)
        if sme["sme_type"] == "internal":
            score *= 1.1

        candidates.append({
            **sme,
            "match_topic": topic,
            "match_mastery": match_mastery,
            "match_score": round(score, 3),
        })

    candidates.sort(key=lambda c: c["match_score"], reverse=True)
    return {
        "topic": topic,
        "learner_id": learner_id,
        "matched_at": datetime.utcnow().isoformat(),
        "match_count": len(candidates[:limit]),
        "matches": candidates[:limit],
        "matches_by_type": {
            "internal": sum(1 for c in candidates[:limit] if c["sme_type"] == "internal"),
            "external": sum(1 for c in candidates[:limit] if c["sme_type"] == "external"),
        },
    }


def book_sme(sme_id: str, learner_id: str, topic: str, slot: str = None) -> dict:
    """
    Create a booking. Returns mock confirmation.
    Phase 2: dual Calendar event + Stripe payment for external + Airtable persistence.
    """
    sme = _get_sme(sme_id)
    if not sme:
        return {"error": f"SME {sme_id} not found"}

    booking = {
        "booking_id": f"booking-{int(datetime.utcnow().timestamp())}",
        "sme_id": sme_id,
        "sme_name": sme["name"],
        "sme_type": sme["sme_type"],
        "learner_id": learner_id,
        "topic": topic,
        "slot": slot or sme.get("next_available", "next available"),
        "duration_minutes": 30,
        "rate_amount": sme.get("rate_per_30min", 0),
        "rate_currency": sme.get("rate_currency"),
        "status": "confirmed",
        "meeting_url": f"https://meet.google.com/booking-{sme_id}-{learner_id}",
        "calendar_event_url": "https://calendar.google.com/...",  # Phase 2: real URL
        "created_at": datetime.utcnow().isoformat(),
    }
    BOOKINGS.append(booking)
    return booking


def list_bookings(learner_id: str) -> dict:
    user_bookings = [b for b in BOOKINGS if b.get("learner_id") == learner_id]
    return {
        "learner_id": learner_id,
        "count": len(user_bookings),
        "bookings": sorted(user_bookings, key=lambda b: b["created_at"], reverse=True),
    }


def register_internal_sme(employee_id: str, profile: dict) -> dict:
    """Backwards-compat wrapper. Forwards to register_sme()."""
    return register_sme(employee_id=employee_id, profile=profile)


def _get_sme(sme_id: str):
    return next(
        (s for s in REGISTERED_SMES + INTERNAL_SMES + EXTERNAL_SMES if s["sme_id"] == sme_id),
        None,
    )


# ──────────────────────────────────────────────────────────────
# Slot picker — parse SME's free-form schedule_window and intersect
# with the learner's calendar busy windows. Used by /sme/find_slots.
# ──────────────────────────────────────────────────────────────

DAY_TOKENS = {
    "mon": 0, "monday": 0,
    "tue": 1, "tues": 1, "tuesday": 1,
    "wed": 2, "wednesday": 2,
    "thu": 3, "thurs": 3, "thursday": 3,
    "fri": 4, "friday": 4,
    "sat": 5, "saturday": 5,
    "sun": 6, "sunday": 6,
}


def parse_schedule_window(text: str) -> list:
    """
    Heuristic parser. Free-form text → list of {weekday, start_hour, end_hour}.

    Handles:
      "Tue/Thu 4-6pm"          → Tue+Thu 16:00-18:00
      "Mon-Fri 9-5"            → Mon-Fri 09:00-17:00
      "Weekdays 10am-12pm"     → Mon-Fri 10:00-12:00
      "Wed PM, 30-min slots"   → Wed 12:00-17:00
      "Mon/Wed evenings"       → Mon+Wed 17:00-21:00
      "" / unparseable         → Mon-Fri 09:00-17:00 (default working week)
    """
    if not text or not text.strip():
        return [{"weekday": d, "start_hour": 9, "end_hour": 17} for d in range(5)]

    s = text.lower()
    days = set()

    # Day ranges
    if re.search(r"(weekday|mon-fri|m-f|monday-friday)", s):
        days.update(range(5))
    if "weekend" in s:
        days.update([5, 6])
    # Day groupings (Tue/Thu, Mon, Wed, etc.)
    for tok, d in DAY_TOKENS.items():
        if re.search(rf"\b{tok}\b", s):
            days.add(d)
    if not days:
        days.update(range(5))

    # Time range
    start_h, end_h = 9, 17
    m = re.search(r"(\d{1,2})\s*(am|pm)?\s*[-–to]+\s*(\d{1,2})\s*(am|pm)?", s)
    if m:
        s1, sm, e1, em = m.groups()
        s1, e1 = int(s1), int(e1)
        sm = sm or em
        em = em or sm
        if sm == "pm" and s1 < 12: s1 += 12
        if em == "pm" and e1 < 12: e1 += 12
        if sm == "am" and s1 == 12: s1 = 0
        if em == "am" and e1 == 12: e1 = 0
        if 0 <= s1 < 24 and 0 <= e1 <= 24 and s1 < e1:
            start_h, end_h = s1, e1
    elif "morning" in s:
        start_h, end_h = 7, 12
    elif "afternoon" in s or re.search(r"\bpm\b", s):
        start_h, end_h = 12, 17
    elif "evening" in s:
        start_h, end_h = 17, 21

    return [{"weekday": d, "start_hour": start_h, "end_hour": end_h} for d in sorted(days)]


def find_slots_for_sme(sme_id: str, learner_id: str, duration_min: int = 30, count: int = 3,
                       window_days: int = 14) -> dict:
    """
    Intersect SME availability with learner's busy windows.
    Returns top-N candidate slots in the same shape as scheduler.compute_free_slots.
    """
    sme = _get_sme(sme_id)
    if not sme:
        return {"error": f"sme {sme_id} not found", "slots": []}

    # Lazy imports — avoid circular at module load (scheduler also lives in services/)
    from . import calendar_client, scheduler

    availability = parse_schedule_window(sme.get("schedule_window") or sme.get("availability_window") or "")
    duration = max(15, int(duration_min or sme.get("preferred_session_length") or 30))

    now = datetime.now(tz.utc)
    window_end = now + timedelta(days=window_days)

    # Learner's busy windows (real Calendar in live mode; deterministic stub otherwise)
    learner_busy = calendar_client.list_busy_windows(learner_id, now, window_end)

    by_weekday = {av["weekday"]: av for av in availability}
    candidates = []
    today = now.date()
    day = today
    while day <= window_end.date():
        av = by_weekday.get(day.weekday())
        if av:
            day_start = datetime.combine(day, datetime.min.time(), tzinfo=tz.utc).replace(hour=av["start_hour"])
            day_end = datetime.combine(day, datetime.min.time(), tzinfo=tz.utc).replace(hour=av["end_hour"])
            if day_end > now:
                free = scheduler._subtract_busy(max(day_start, now), day_end, learner_busy)
                for free_start, free_end in free:
                    candidates.extend(scheduler._slice_slot(free_start, free_end, duration))
        day += timedelta(days=1)

    # Score: sooner is better; small midday bonus
    def score(slot):
        start = slot["start"]
        score = 0.5
        days_out = (start.date() - today).days
        score += max(0, 0.4 - 0.05 * days_out)
        if 10 <= start.hour <= 14:
            score += 0.1
        return round(min(1.0, score), 3)

    ranked = sorted(
        ({**c, "score": score(c)} for c in candidates),
        key=lambda c: (-c["score"], c["start"]),
    )
    serialized = [scheduler._serialize_slot(c, today) for c in ranked[:count]]

    return {
        "sme_id": sme_id,
        "sme_name": sme.get("name"),
        "sme_role": sme.get("role"),
        "topics": sme.get("topics") or [],
        "duration_min": duration,
        "schedule_window_text": sme.get("schedule_window") or sme.get("availability_window") or "",
        "schedule_window_parsed": availability,
        "slots": serialized,
        "calendar_connected": calendar_client.is_connected(),
        "expectations_from_students": sme.get("expectations_from_students", ""),
        "rate_label": _rate_label(sme),
    }


def _rate_label(sme: dict) -> str:
    rm = sme.get("rate_model")
    if rm == "free":
        return "Free"
    if rm == "paid" or (sme.get("rate_per_30min") or 0) > 0:
        return f"{(sme.get('rate_currency') or 'usd').upper()} {sme.get('rate_per_30min', 0)}/30 min"
    return "Kudos only"


def book_slot_with_sme(sme_id: str, learner_id: str, topic: str,
                       start_at: str, end_at: str) -> dict:
    """
    Confirm a booked SME session. Creates dual Google Calendar events
    (one on the learner's calendar, one on the SME's), persists a
    booking row that mirrors Table 20 shape.

    Both calendar.insert calls degrade to stub mode when no service
    account is configured — same pattern as Project Manager Mode.
    """
    sme = _get_sme(sme_id)
    if not sme:
        return {"error": f"sme {sme_id} not found"}
    if not start_at or not end_at:
        return {"error": "start_at and end_at required (ISO 8601)"}

    from . import calendar_client

    start_dt = datetime.fromisoformat(start_at.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_at.replace("Z", "+00:00"))
    title = f"📚 Aasan SME — {sme.get('name')} · {topic or (sme.get('topics') or ['session'])[0]}"
    description = (
        f"Topic: {topic}\n\n"
        f"Read this before our session:\n"
        f"{sme.get('expectations_from_students') or '(no specific prep instructions)'}\n\n"
        f"SME bio: {sme.get('bio', '')}"
    )

    learner_event = calendar_client.insert_event(learner_id, title, start_dt, end_dt, description)

    sme_calendar_id = (
        sme.get("employee_id")
        or sme.get("external_email")
        or f"sme-{sme_id}"
    )
    sme_event = calendar_client.insert_event(sme_calendar_id, title, start_dt, end_dt, description)

    booking = {
        "booking_id": len(BOOKINGS) + 1,
        "sme_id": sme_id,
        "sme_name": sme.get("name"),
        "learner_id": learner_id,
        "topic": topic or (sme.get("topics") or ["session"])[0],
        "scheduled_at": start_at,
        "end_at": end_at,
        "duration_minutes": int((end_dt - start_dt).total_seconds() // 60),
        "rate_amount": sme.get("rate_per_30min", 0),
        "rate_currency": sme.get("rate_currency"),
        "status": "confirmed",
        "calendar_event_id_learner": learner_event["event_id"],
        "calendar_event_id_sme": sme_event["event_id"],
        "calendar_event_url": learner_event["event_url"],
        "meeting_url": f"https://meet.google.com/booking-{sme_id}-{int(start_dt.timestamp())}",
        "mode": learner_event.get("mode", "live"),
        "created_at": datetime.utcnow().isoformat(),
    }
    BOOKINGS.append(booking)
    return booking
