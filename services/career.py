"""
Career Compass / Market Watch orchestration.

Three independent watcher pipelines, all running through Perplexity Computer:

  1. Role-market scan  — scrape job postings for the learner's target role(s),
                         derive skill-demand benchmarks, surface deltas
  2. Course-launch scan — poll training-content sources for new courses
                          relevant to the learner's domain
  3. Vendor-cert scan   — monitor vendor portals (AWS, Google, Microsoft, Anthropic,
                          Nvidia, Salesforce) for new certifications + cert path updates

Each produces Career_Signals (matching Data Model Table 24) that surface in
the learner's weekly digest.

PHASE 1 STORAGE
───────────────
Demo target-role + source list is hardcoded below. Phase 2 reads
Market_Watches (Table 22) from Airtable to know each learner's subscriptions.

STUB MODE
─────────
When PERPLEXITY_API_KEY is unset, the orchestrator still produces realistic
digest entries (not just generic "stub" placeholders) so the demo is
demonstrable end-to-end. Real calls activate the moment the env var is set.
"""

from datetime import datetime
from . import perplexity_client, claude_client


# Hardcoded demo subscription — replace with Market_Watches query in Phase 2
DEMO_TARGET_ROLE = "Senior Cloud Architect"
DEMO_PEER_COMPANIES = ["Stripe", "Snowflake", "Datadog", "Databricks", "MongoDB", "Confluent", "HashiCorp", "Vercel"]
DEMO_LEARNER_SKILLS = ["Kubernetes", "AWS", "Container orchestration", "Infrastructure as code"]
DEMO_DOMAINS = ["Cloud Infrastructure", "DevOps", "AI/ML"]

JOB_BOARD_SOURCES = [
    "https://www.linkedin.com/jobs/search/",
    "https://www.indeed.com/jobs",
    "https://wellfound.com/jobs",
]

TRAINING_SOURCES = [
    "https://www.coursera.org/courses",
    "https://www.linkedin.com/learning",
    "https://www.udemy.com/courses",
    "https://maven.com/courses",
]

VENDOR_CERT_SOURCES = [
    "https://aws.amazon.com/certification/",
    "https://cloud.google.com/learn/certification",
    "https://learn.microsoft.com/certifications",
    "https://www.anthropic.com/learn",
    "https://learn.nvidia.com",
    "https://trailhead.salesforce.com",
]


def run_scan(user_id: str = None, target_role: str = None, max_signals: int = 10) -> dict:
    """
    Top-level scan. Runs all three pipelines, returns aggregated Career_Signals.

    Phase 1: uses DEMO_TARGET_ROLE if target_role not provided.
    """
    role = target_role or DEMO_TARGET_ROLE

    # Run the three pipelines
    role_signals = _scan_role_market(role)
    course_signals = _scan_course_launches(DEMO_LEARNER_SKILLS, DEMO_DOMAINS)
    cert_signals = _scan_vendor_certs(role)

    all_signals = role_signals + course_signals + cert_signals

    # Sort by relevance + cap at max_signals
    all_signals.sort(key=lambda s: s.get("relevance_score", 0), reverse=True)
    top_signals = all_signals[:max_signals]

    return {
        "user_id": user_id,
        "target_role": role,
        "scanned_at": datetime.utcnow().isoformat(),
        "signals_count": len(top_signals),
        "signals_by_type": {
            "role_skill_shift": sum(1 for s in top_signals if s["signal_type"] == "role_skill_shift"),
            "new_course": sum(1 for s in top_signals if s["signal_type"] == "new_course"),
            "vendor_cert": sum(1 for s in top_signals if s["signal_type"] == "vendor_cert"),
        },
        "signals": top_signals,
        "modes": {
            "computer": "live" if perplexity_client.is_live() else "stub",
            "classifier": "live" if claude_client.is_live() else "stub",
        },
    }


# ──────────────────────────────────────────────────────────────
# Pipeline 1 — Role-market scan
# Scrapes job boards for the target role; aggregates skill demand
# ──────────────────────────────────────────────────────────────

def _scan_role_market(role: str) -> list:
    if not perplexity_client.is_live():
        return _stub_role_market_signals(role)

    # Real path: scrape postings, then classify aggregated skill demand
    scrape = perplexity_client.scrape_pattern(
        query=f"{role} job posting required skills",
        sources=JOB_BOARD_SOURCES,
        max_results=50,
    )
    if scrape.get("status") != "ok":
        return []

    # Aggregate skill mentions via Claude
    # (Phase 2: build full Role_Benchmarks; Phase 1: just surface top deltas)
    results = scrape.get("result", {}).get("results", [])
    if not results:
        return []

    # Stub-friendly: produce one signal per peer-company-trend we detect
    return _aggregate_role_signals(role, results)


def _aggregate_role_signals(role: str, scrape_results: list) -> list:
    """Phase 1 placeholder — real impl uses Claude over the postings text."""
    return [{
        "signal_type": "role_skill_shift",
        "title": f"Skill demand shifts detected for {role}",
        "body": f"{len(scrape_results)} postings analyzed across peer companies. See benchmarks for full breakdown.",
        "relevance_score": 0.7,
        "content_ref": None,
        "detected_at": datetime.utcnow().isoformat(),
    }]


# ──────────────────────────────────────────────────────────────
# Pipeline 2 — Course-launch scan
# ──────────────────────────────────────────────────────────────

def _scan_course_launches(skills: list, domains: list) -> list:
    if not perplexity_client.is_live():
        return _stub_course_launch_signals(skills, domains)

    scrape = perplexity_client.scrape_pattern(
        query=f"new courses launched on {' OR '.join(skills)}",
        sources=TRAINING_SOURCES,
        max_results=20,
    )
    if scrape.get("status") != "ok":
        return []

    results = scrape.get("result", {}).get("results", [])
    return [_course_to_signal(r, skills) for r in results[:5]]


def _course_to_signal(result: dict, skills: list) -> dict:
    return {
        "signal_type": "new_course",
        "title": f"New course: {result.get('title', 'Untitled')}",
        "body": result.get("snippet", ""),
        "relevance_score": 0.6,  # Phase 2: Claude scores against learner goals
        "content_ref": result.get("url"),
        "detected_at": datetime.utcnow().isoformat(),
    }


# ──────────────────────────────────────────────────────────────
# Pipeline 3 — Vendor cert scan
# ──────────────────────────────────────────────────────────────

def _scan_vendor_certs(role: str) -> list:
    if not perplexity_client.is_live():
        return _stub_vendor_cert_signals(role)

    scrape = perplexity_client.scrape_pattern(
        query=f"new certification announcement {role}",
        sources=VENDOR_CERT_SOURCES,
        max_results=10,
    )
    if scrape.get("status") != "ok":
        return []

    results = scrape.get("result", {}).get("results", [])
    return [_cert_to_signal(r) for r in results[:3]]


def _cert_to_signal(result: dict) -> dict:
    return {
        "signal_type": "vendor_cert",
        "title": result.get("title", "New certification"),
        "body": result.get("snippet", ""),
        "relevance_score": 0.5,
        "content_ref": result.get("url"),
        "detected_at": datetime.utcnow().isoformat(),
    }


# ──────────────────────────────────────────────────────────────
# Stub responses — realistic-looking digest entries so the demo
# is demonstrable without PERPLEXITY_API_KEY. Same shape as live.
# ──────────────────────────────────────────────────────────────

def _stub_role_market_signals(role: str) -> list:
    return [
        {
            "signal_type": "role_skill_shift",
            "title": f"FinOps now required for {role} at peer companies",
            "body": (
                f"47% of {role} postings at 8 peer companies (Stripe, Snowflake, Datadog, "
                f"Databricks, MongoDB, Confluent, HashiCorp, Vercel) now list FinOps experience "
                f"as required — up from 12% a year ago. Add a FinOps mini-path? (~6 hours)"
            ),
            "relevance_score": 0.92,
            "content_ref": "stub-benchmark-finops-2026-04",
            "detected_at": datetime.utcnow().isoformat(),
            "_stub": True,
        },
        {
            "signal_type": "role_skill_shift",
            "title": f"Multi-region resilience moving from 'nice-to-have' to required",
            "body": (
                f"31% of {role} postings now require multi-region experience (was 8% in Q3 2025). "
                f"Sharp acceleration tied to GenAI infrastructure scale-out."
            ),
            "relevance_score": 0.85,
            "content_ref": "stub-benchmark-multiregion-2026-04",
            "detected_at": datetime.utcnow().isoformat(),
            "_stub": True,
        },
    ]


def _stub_course_launch_signals(skills: list, domains: list) -> list:
    return [
        {
            "signal_type": "new_course",
            "title": "Anthropic launched: Building Production Agentic Systems",
            "body": (
                "4 hours · 4.8 rating · matches your AI/ML exploration goal. "
                "Covers tool use, multi-step planning, evaluation harnesses. New this week."
            ),
            "relevance_score": 0.88,
            "content_ref": "stub-anthropic-agentic-2026-04",
            "detected_at": datetime.utcnow().isoformat(),
            "_stub": True,
        },
        {
            "signal_type": "new_course",
            "title": "Coursera launched: GitOps with ArgoCD — Hands-on Labs",
            "body": (
                "8 hours · matches your Cloud Architect path · partners with CNCF. "
                "Covers progressive delivery, multi-cluster GitOps, ApplicationSet."
            ),
            "relevance_score": 0.78,
            "content_ref": "stub-coursera-gitops-2026-04",
            "detected_at": datetime.utcnow().isoformat(),
            "_stub": True,
        },
        {
            "signal_type": "new_course",
            "title": "LinkedIn Learning: FinOps for Engineers (Foundational)",
            "body": (
                "2 hours · directly relevant to the FinOps demand shift detected for your "
                "target role. Probably the fastest way to fill that gap."
            ),
            "relevance_score": 0.95,  # Highest — closes a known role gap
            "content_ref": "stub-linkedin-finops-2026-04",
            "detected_at": datetime.utcnow().isoformat(),
            "_stub": True,
        },
    ]


def _stub_vendor_cert_signals(role: str) -> list:
    return [
        {
            "signal_type": "vendor_cert",
            "title": "AWS announced new SA Pro path with 3 new modules",
            "body": (
                "You've already covered 2 of the 3 new modules (multi-region failover, "
                "advanced VPC). The third is on cost optimization — pairs with the FinOps signal above."
            ),
            "relevance_score": 0.83,
            "content_ref": "stub-aws-sapro-2026-04",
            "detected_at": datetime.utcnow().isoformat(),
            "_stub": True,
        },
        {
            "signal_type": "vendor_cert",
            "title": "Google Cloud Professional Cloud Architect — exam refresh",
            "body": (
                "Q2 2026 refresh adds AI infrastructure design as a formal exam domain. "
                "If you're considering GCP-PCA after AWS SA Pro, this is the new shape."
            ),
            "relevance_score": 0.68,
            "content_ref": "stub-gcp-pca-refresh-2026-04",
            "detected_at": datetime.utcnow().isoformat(),
            "_stub": True,
        },
    ]
