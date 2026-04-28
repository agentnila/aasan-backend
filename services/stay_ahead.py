"""
Stay Ahead — career mobility intelligence (sub-capability of Career Compass).

WHY THIS EXISTS
───────────────
Career Compass V1 (services/career.py) recommends LEARNING — what skills are
shifting, what courses are launching, what certs are new.

Stay Ahead answers a different set of questions, with the tech downturn as
a real-world driver:

  1. "How do I find my next job?" — best-fit roles I could apply for today
  2. "What jobs have transferable experience that can apply?" — pivot options
  3. "What jobs do I have best chance for now?" — match-score-ranked list
  4. "How can I change my trajectory if I don't like where I am?" — explicit pivot paths
  5. "Am I in a vulnerable position?" — market risk signal for current role/stack

KEY EXPANSION
─────────────
Beyond training. Stay Ahead also recommends EXPERIENCES — hands-on, often
unpaid, that build resume signal training alone can't:
  - Adjunct lecturer at a local college
  - Personal AI project (even for non-programmers — vibe-coding)
  - Open-source contribution to a project in your domain
  - Volunteer board/advisor role for a non-profit
  - Public technical writing / podcast
  - Cross-functional internal stretch project

PIPELINE
────────
  Perplexity Computer scrapes:
    - Job boards for postings matching learner's role & adjacent roles
    - Industry layoff trackers + hiring trends for risk signal
    - Volunteer / adjunct / OSS opportunity feeds
  Claude reasons over:
    - Learner profile (knowledge graph + skills + goals)
    - Scrape results
    - Maps transferable skills → adjacent roles
    - Suggests experiences that close the gap to specific stretch roles
"""

from datetime import datetime
from . import perplexity_client, claude_client


# Hardcoded learner profile for demo (Phase 2: read from Neo4j + Mem0)
DEMO_PROFILE = {
    "current_role": "Senior Software Engineer",
    "current_company": "TechCorp",
    "stated_goal": "Become a Cloud Architect by Q4 2026",
    "tenure_years": 4,
    "core_skills": ["AWS", "Kubernetes", "Linux", "Containers", "Python", "Service Mesh"],
    "adjacent_skills": ["Observability", "Networking", "IAM"],
    "stated_interests": ["AI/ML", "Cloud Architecture", "Platform Engineering"],
    "reading_patterns": ["agentic systems", "multi-region resilience", "FinOps"],
    "location": "Remote (US)",
}


def run_stay_ahead(user_id: str = None, profile: dict = None) -> dict:
    """
    Top-level Stay Ahead scan. Produces a 5-section career mobility digest.
    """
    profile = profile or DEMO_PROFILE

    if perplexity_client.is_live():
        return _live_stay_ahead(user_id, profile)
    return _stub_stay_ahead(user_id, profile)


# ──────────────────────────────────────────────────────────────
# Live path — Perplexity Computer + Claude reasoning
# ──────────────────────────────────────────────────────────────

def _live_stay_ahead(user_id: str, profile: dict) -> dict:
    """
    Real path: Computer scrapes job boards + risk signals; Claude reasons.
    Intentionally minimal Phase 1 — full implementation Week 7-8.
    """
    # 1. Scrape current-role + adjacent-role postings
    role_scrape = perplexity_client.scrape_pattern(
        query=f"{profile['current_role']} job posting required skills",
        sources=["https://www.linkedin.com/jobs", "https://www.indeed.com/jobs", "https://wellfound.com/jobs"],
        max_results=30,
    )
    # 2. Scrape risk signals (layoff trackers, role-demand trends)
    risk_scrape = perplexity_client.scrape_pattern(
        query=f"layoffs hiring trends {profile['current_role']} 2026",
        sources=["https://layoffs.fyi", "https://news.ycombinator.com"],
        max_results=10,
    )

    # 3. Claude reasoning over (profile + scrape results) → 5-section digest
    # Phase 2: full Claude prompt. For now, fall back to stub if scrape returned anything.
    return _stub_stay_ahead(user_id, profile, modes={"computer": "live", "classifier": "stub"})


# ──────────────────────────────────────────────────────────────
# Stub — narratively coherent demo for Sarah Chen profile
# ──────────────────────────────────────────────────────────────

def _resilience_band(score):
    """Map a 0-100 resilience score to a category band.
    Calibrated to where most knowledge workers actually land (median ~60-65)."""
    if score >= 80:
        return {"label": "AI-Resilient", "tone": "green", "verdict": "Strong position. Top-quartile resilience. Stay sharp; market values your trajectory."}
    if score >= 65:
        return {"label": "Stable", "tone": "green", "verdict": "Well-positioned. A few specific moves keep you ahead of the curve."}
    if score >= 50:
        return {"label": "At Risk", "tone": "amber", "verdict": "Vulnerable to displacement within 18-24 months. Act this quarter — Stay Ahead has the moves."}
    return {"label": "High Risk", "tone": "red", "verdict": "Significant displacement risk in 12 months. Pivot or upskill now — see the moves below."}


def _stub_stay_ahead(user_id: str, profile: dict, modes: dict = None) -> dict:
    modes = modes or {"computer": "stub", "classifier": "stub"}

    # AI-Resilience scoring — V3 headline metric
    # vulnerability_score: 0 = AI-immune, 1 = high replacement risk (legacy field, kept for back-compat)
    # ai_resilience_score: 0-100, HIGHER IS BETTER. The headline number people remember.
    vulnerability_score = 0.32
    ai_resilience_score = round((1 - vulnerability_score) * 100)  # 68
    band = _resilience_band(ai_resilience_score)

    # Trend — last 4 quarters (stub: gentle improvement reflecting Sarah's K8s + cloud track)
    score_history = [
        {"quarter": "Q2 2025", "score": 58},
        {"quarter": "Q3 2025", "score": 62},
        {"quarter": "Q4 2025", "score": 64},
        {"quarter": "Q1 2026", "score": 66},
        {"quarter": "Q2 2026", "score": ai_resilience_score},
    ]

    # Per-component breakdown — what makes up the composite
    components = [
        {"label": "Skill demand growth", "score": 78, "note": "Cloud Architect demand +18% YoY in your market"},
        {"label": "AI task replacement risk", "score": 62, "note": "Some routine tasks (boilerplate, Terraform drafting) automatable today"},
        {"label": "Up-the-stack mobility", "score": 72, "note": "Architectural judgment + customer-facing decisions are AI-resistant"},
        {"label": "Geographic + financial mobility", "score": 65, "note": "Remote-friendly stack; comp at 75th percentile"},
        {"label": "Cross-skill versatility", "score": 70, "note": "K8s + AWS + Linux + Python + presentation skills"},
    ]

    # Peer benchmark
    peer_benchmark = {
        "role": "Senior SWE (cloud track)",
        "market": "US tech, 2026",
        "peer_avg_score": 58,
        "your_score": ai_resilience_score,
        "delta_vs_peers": ai_resilience_score - 58,
        "percentile": 73,
    }

    return {
        "user_id": user_id,
        "profile": profile,
        "scanned_at": datetime.utcnow().isoformat(),
        # AI-Resilience — V3 headline metric
        # Higher = less at-risk from AI or AI-built tooling
        "ai_resilience": {
            # NEW V3 — headline score (higher = better)
            "score": ai_resilience_score,
            "score_max": 100,
            "band": band["label"],
            "band_tone": band["tone"],
            "band_verdict": band["verdict"],
            "trend": {
                "history": score_history,
                "direction": "rising",
                "change_last_quarter": ai_resilience_score - score_history[-2]["score"],
                "narrative": (
                    f"Your AI-Resilience score went {score_history[-2]['score']} → {ai_resilience_score} "
                    f"this quarter — your move into agent-aware architecture work is paying off. "
                    f"Sustained trajectory: +{ai_resilience_score - score_history[0]['score']} over 4 quarters."
                ),
            },
            "components": components,
            "peer_benchmark": peer_benchmark,
            # Legacy + descriptive fields
            "vulnerability_score": vulnerability_score,
            "vulnerability_level": "low-medium",  # back-compat
            "headline": (
                "Your Senior SWE → Cloud Architect track is RELATIVELY AI-resilient. "
                "Architectural judgment, organizational alignment, and customer-facing decisions "
                "are the parts of your job AI is NOT taking over. Cloud Architects are needed to "
                "DESIGN the infrastructure AI agents run on — demand growing 22% YoY."
            ),
            "ai_replaced_today": [
                "Boilerplate code generation (use AI; don't define yourself by it)",
                "Initial Terraform / CloudFormation drafting",
                "Routine log analysis + first-pass incident triage",
                "Documentation drafts + API reference lookup",
            ],
            "ai_amplified_skills": [
                "System design judgment (AI proposes, you decide tradeoffs)",
                "Code review at depth (AI flags, you reason about implications)",
                "Customer-facing architecture decisions",
                "Cross-team alignment + organizational politics",
                "Incident command (humans still own the call under pressure)",
            ],
            "up_the_stack_moves": [
                {
                    "title": "Build agentic systems yourself",
                    "what": "Be the engineer who designs + ships AI agents — not the one whose job is replaced by them",
                    "concrete_step": "Build a personal AI agent project this quarter (already in your hands-on experiences below)",
                },
                {
                    "title": "Become the AI infra architect",
                    "what": "Cloud Architects who specialize in AI workload infrastructure are in extreme demand. Vector DBs, GPU orchestration, multi-region inference",
                    "concrete_step": "Add 'AI infrastructure patterns' to your Cloud Architect path",
                },
                {
                    "title": "Move into agent supervision / evaluation",
                    "what": "Designing the EVALS for AI agents and supervising agentic workflows is a net-new role category. Few people are good at it yet.",
                    "concrete_step": "Take Anthropic's agentic systems course (already in your Career Compass digest)",
                },
            ],
            "ai_resilient_pivots": [
                {
                    "role": "AI Infrastructure Architect",
                    "rationale": "AI is a tailwind. Demand growing 35% YoY. Your cloud expertise translates directly.",
                },
                {
                    "role": "Agentic Systems Engineer",
                    "rationale": "Net-new role; supply is thin. AI doesn't replace you — you design what it does.",
                },
                {
                    "role": "Developer Advocate (AI tools)",
                    "rationale": "AI tools companies need humans who can teach + show, at scale. AI-immune for at least 5 years.",
                },
            ],
            "what_to_avoid": [
                "Roles defined by routine code generation (AI is at parity already)",
                "Pure DevOps automation specialist (AI agents will own end-to-end pipelines within 18 months)",
                "QA-only roles (AI test generation is rapidly maturing)",
            ],
        },
        "market_risk": {
            "level": "manageable",
            "signal": (
                f"Your trajectory toward Cloud Architect is well-timed — demand +18% YoY in your market. "
                "BUT: AWS-specialist roles are shrinking 6% YoY. Diversify to multi-cloud in the next "
                "12-18 months or risk becoming a niche specialist with declining options."
            ),
            "tone": "amber",
            "data_points": [
                "Senior Cloud Architect postings: 47% require multi-cloud (was 22% in Q3 2025)",
                "AWS-only specialist postings: -6% YoY",
                "FinOps as required skill: 47% (was 12% a year ago)",
                "Tech sector layoffs (last 90d): ~38K reported; cloud/platform roles relatively insulated",
            ],
        },
        "best_fit_roles": [
            {
                "title": "Senior Cloud Engineer", "company": "Stripe", "location": "Remote", "salary_range": "$220K-$260K",
                "match_score": 0.92, "match_reason": "Your AWS + K8s + Linux + Python match 9 of 10 listed requirements. Application-ready today.",
                "why_apply": "Strong infra culture, public-good revenue model, growing platform team.",
            },
            {
                "title": "Senior Site Reliability Engineer", "company": "Datadog", "location": "NYC / remote", "salary_range": "$240K-$290K",
                "match_score": 0.89, "match_reason": "Strong overlap with your gap-detection + observability work. Datadog values K8s ops experience heavily.",
                "why_apply": "If you want depth in observability before broadening to architect.",
            },
            {
                "title": "Platform Engineering Lead", "company": "Snowflake", "location": "Remote", "salary_range": "$260K-$310K",
                "match_score": 0.86, "match_reason": "Tech match strong. The 'Lead' part stretches your management exposure (slight gap).",
                "why_apply": "Step into leadership while keeping technical depth. Snowflake is hiring aggressively.",
            },
        ],
        "stretch_roles": [
            {
                "title": "Staff Cloud Architect", "company": "Anthropic", "location": "SF / remote", "salary_range": "$280K-$340K",
                "match_score": 0.78, "match_reason": "Tech match strong. Gaps: FinOps experience (you have a watch on this) + multi-region production scale.",
                "path_to_ready": "6 weeks if you finish FinOps mini-path + 1 multi-region side project. Then strong fit.",
            },
            {
                "title": "Engineering Manager", "company": "Vercel", "location": "Remote", "salary_range": "$250K-$310K",
                "match_score": 0.71, "match_reason": "Tech depth fine. Gap: explicit management/people experience (you've led projects, not directly managed).",
                "path_to_ready": "12 months on your current track + lead a cross-functional initiative + take on first direct reports.",
            },
        ],
        "pivot_options": [
            {
                "title": "Solutions Engineer", "company": "AWS", "location": "Remote / hybrid", "salary_range": "$200K-$280K (base + variable)",
                "match_score": 0.74, "transferable_skills": ["Cloud expertise", "Customer-facing comfort", "Technical communication"],
                "why_pivot": "Higher upside via variable comp. Less on-call. Travels well to consulting / VC roles later. Big career-options unlock.",
            },
            {
                "title": "Developer Advocate", "company": "HashiCorp", "location": "Remote", "salary_range": "$180K-$240K",
                "match_score": 0.68, "transferable_skills": ["Cloud expertise", "Content creation interest", "Technical depth"],
                "why_pivot": "Public visibility builds personal brand. Path into VC / founding role / TPM. Best if you'll enjoy talks + writing.",
            },
            {
                "title": "Technical Product Manager (Infrastructure)", "company": "Datadog / Stripe / similar", "location": "Remote", "salary_range": "$220K-$300K",
                "match_score": 0.65, "transferable_skills": ["Deep technical context", "Cross-functional thinking", "Customer empathy"],
                "why_pivot": "PM is a natural exit ramp from senior IC. Tech PM specifically needs people who actually understand infra.",
            },
        ],
        "hands_on_experiences": [
            {
                "title": "Adjunct lecturer in Cloud Architecture", "kind": "teaching",
                "adds_to_resume": "Teaching credibility · networking with academia · pivot to dev-advocacy or content",
                "how_to_get_it": "Most CS departments at nearby universities accept industry adjuncts for one-off seminars or semester courses. Email the dept chair. Time: 2-4 hrs/week during semester.",
                "fit_score": 0.88,
            },
            {
                "title": "Build a personal AI agent project (ship to GitHub)", "kind": "side_project",
                "adds_to_resume": "AI/ML adjacency · public artifact · matches your stated MLOps exploration · proves you can build, not just architect",
                "how_to_get_it": "Weekend project. Use Claude API + a real personal pain. Ship. Write a blog post. Time: 3-5 weekends.",
                "fit_score": 0.92,
            },
            {
                "title": "Open-source contribution to KubeVirt or Cilium", "kind": "open_source",
                "adds_to_resume": "Deep K8s credibility · visible to hiring managers · directly fits Cloud Architect goal",
                "how_to_get_it": "Pick a 'good first issue' tagged in repo. Ask in their Slack. Time: 4-6 weekends to merge first PR.",
                "fit_score": 0.85,
            },
            {
                "title": "Volunteer board advisor / tech advisor for a non-profit", "kind": "advisor",
                "adds_to_resume": "Leadership · governance · cross-functional · resume diversity for management roles",
                "how_to_get_it": "VolunteerMatch, BoardSource, or local non-profit listings. Look for orgs needing infra help. Time: 2-4 hrs/month.",
                "fit_score": 0.76,
            },
            {
                "title": "Public technical blog series on multi-region patterns", "kind": "content",
                "adds_to_resume": "Personal brand for FinOps/multi-region pivot · evidence of architectural thinking · indexed by hiring managers",
                "how_to_get_it": "5-post series, weekly cadence. Substack or your own domain. Cross-post on Hacker News. Time: 3-4 hrs per post.",
                "fit_score": 0.81,
            },
        ],
        "summary": (
            f"You're well-positioned for {profile['stated_goal']}. Three roles you could land today, "
            f"two stretch roles within reach, and three viable pivots if you want to change direction. "
            f"The biggest leverage move right now: a hands-on AI project + adjunct lecturing — both "
            f"build credentials your training alone can't."
        ),
        "modes": modes,
        "_stub": modes.get("computer") == "stub",
    }
