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

from datetime import datetime, timedelta


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


def find_smes(topic: str, learner_id: str = None, limit: int = 5) -> dict:
    """
    Match SMEs against a topic. Returns ranked list with internal first
    (cheaper / more available), then external by relevance.

    Ranking signal: (topic_mastery × opted_in × availability_recency × kudos_score)
    """
    topic_lower = topic.lower()
    candidates = []

    for sme in INTERNAL_SMES + EXTERNAL_SMES:
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
    """
    Opt-in flow for an internal employee to register as an SME.
    Phase 2: writes to Airtable SME_Profiles + flips opted_in=True.
    """
    return {
        "sme_id": f"internal-{employee_id}",
        "registered_at": datetime.utcnow().isoformat(),
        "profile": profile,
        "status": "opt_in_pending_approval",  # Phase 2: skip approval, flip immediately
        "_note": "Phase 1: this is a stub. Phase 2 writes to Airtable + activates the SME.",
    }


def _get_sme(sme_id: str):
    return next((s for s in INTERNAL_SMES + EXTERNAL_SMES if s["sme_id"] == sme_id), None)
