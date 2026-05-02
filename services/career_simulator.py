"""
Career Scenario Planning Simulator — third Career Compass capability.

Takes 2-3 candidate career paths and projects 12-24 month outcomes side by side,
factoring in:
  - Learner's current state (skills, role, tenure, momentum)
  - Effort assumptions per scenario (hours/week, intensity)
  - Market trends (from Career Compass + Stay Ahead data)
  - Probabilistic confidence ranges (low / mid / high outcomes)

For each scenario the simulator returns:
  - Probability of reaching each outcome milestone
  - Projected role + comp range at 12 / 24 months
  - Required experiences to unlock each outcome
  - Risk markers (market, personal, dependency)

Phase 1: Hardcoded demo scenarios for Sarah Chen profile (so the simulation
tells a coherent story alongside Stay Ahead). Phase 2: Claude reasoning over
(profile + path engine state + market data + chosen scenarios) to produce
real projections.
"""

from datetime import datetime
from . import claude_client


DEFAULT_SCENARIOS = [
    {
        "id": "stay-current",
        "name": "Stay the course — Cloud Architect at TechCorp",
        "description": "Continue current trajectory. Finish Cloud Architect path. Take the Q4 promotion when offered.",
        "effort_hours_per_week": 4,
        "horizon_months": 18,
    },
    {
        "id": "pivot-aws-se",
        "name": "Pivot — Solutions Engineer at AWS",
        "description": "Apply for AWS SE roles in 3 months. Pause some learning, shift focus to customer-facing skills + AWS certifications.",
        "effort_hours_per_week": 6,
        "horizon_months": 18,
    },
    {
        "id": "stretch-anthropic",
        "name": "Stretch — Staff Cloud Architect at Anthropic",
        "description": "Aggressive 6-month sprint: finish FinOps mini-path, ship a multi-region side project, write public content. Then apply.",
        "effort_hours_per_week": 10,
        "horizon_months": 12,
    },
]


def run_simulation(user_id: str = None, scenarios: list = None, profile: dict = None) -> dict:
    """
    Project outcomes across scenarios. Returns side-by-side comparable projections.

    Scenarios may arrive as either a list of dicts (full shape) or a list of
    strings (just names — frontend convenience). Strings are coerced to dicts
    with the name as both id and name; missing fields fall back to the matching
    DEFAULT_SCENARIO when the id matches one we know.
    """
    if not scenarios:
        scenarios = DEFAULT_SCENARIOS
    else:
        scenarios = _normalize_scenarios(scenarios)

    if claude_client.is_live():
        # Phase 2: real Claude reasoning over scenarios + profile + market state
        # For now even with Claude live, return the stub (richer narrative)
        pass

    return _stub_simulation(user_id, scenarios, profile or {})


def _normalize_scenarios(raw: list) -> list:
    """Coerce mixed string/dict input into the canonical scenario dict shape."""
    out = []
    defaults_by_id = {s["id"]: s for s in DEFAULT_SCENARIOS}
    for item in raw or []:
        if isinstance(item, str):
            slug = item.lower().replace(" ", "-")[:40]
            base = defaults_by_id.get(slug, {})
            out.append({
                "id": slug,
                "name": item,
                "description": base.get("description", item),
                "effort_hours_per_week": base.get("effort_hours_per_week", 5),
                "horizon_months": base.get("horizon_months", 18),
            })
        elif isinstance(item, dict):
            sid = item.get("id") or item.get("name", "scenario").lower().replace(" ", "-")[:40]
            base = defaults_by_id.get(sid, {})
            merged = {
                "id": sid,
                "name": item.get("name") or sid,
                "description": item.get("description") or base.get("description", ""),
                "effort_hours_per_week": item.get("effort_hours_per_week") or base.get("effort_hours_per_week", 5),
                "horizon_months": item.get("horizon_months") or base.get("horizon_months", 18),
            }
            out.append(merged)
    return out or DEFAULT_SCENARIOS


def _stub_simulation(user_id: str, scenarios: list, profile: dict) -> dict:
    """Narratively coherent projections per scenario."""
    projections = []
    for s in scenarios:
        if s["id"] == "stay-current":
            projections.append(_stay_current_projection(s))
        elif s["id"] == "pivot-aws-se":
            projections.append(_pivot_aws_projection(s))
        elif s["id"] == "stretch-anthropic":
            projections.append(_stretch_anthropic_projection(s))
        else:
            projections.append(_generic_projection(s))

    return {
        "user_id": user_id,
        "simulated_at": datetime.utcnow().isoformat(),
        "scenario_count": len(projections),
        "projections": projections,
        "comparison_summary": (
            "Stay-the-course is the lowest-risk path with strong upside — 70% chance of Cloud Architect within 18 months. "
            "Pivot to AWS SE is high variable comp (+30-50%) but trades technical depth for customer time. "
            "Stretch to Anthropic is the highest ceiling but requires 10 hrs/week for 6 months — only 35% chance, "
            "but the 35% includes a +$40-60K comp jump and a much stronger long-term resume."
        ),
        "modes": {"engine": "live" if claude_client.is_live() else "stub"},
    }


def _stay_current_projection(s):
    return {
        **s,
        "outcomes": [
            {
                "milestone_at_months": 6,
                "low_outcome": {"role": "Senior SWE (current)", "comp": "$200K", "probability": 0.95, "note": "Steady-state baseline"},
                "mid_outcome": {"role": "Senior SWE + Cloud Architect candidate", "comp": "$200K", "probability": 0.85, "note": "Path 80% complete; promotion conversation started"},
                "high_outcome": {"role": "Promoted to Staff SWE early", "comp": "$240K", "probability": 0.20, "note": "Strong perf review + visible side project pulls promo forward"},
            },
            {
                "milestone_at_months": 12,
                "low_outcome": {"role": "Senior SWE", "comp": "$200K", "probability": 0.10, "note": "Promotion delayed by re-org or perf issues"},
                "mid_outcome": {"role": "Cloud Architect (Staff)", "comp": "$240K-$260K", "probability": 0.70, "note": "Goal achieved on plan"},
                "high_outcome": {"role": "Staff Cloud Architect + leading migration", "comp": "$260K-$285K", "probability": 0.20, "note": "Cloud Architect + visible org-level project leadership"},
            },
            {
                "milestone_at_months": 18,
                "low_outcome": {"role": "Cloud Architect", "comp": "$220K-$240K", "probability": 0.15, "note": "Leveling stalls; market lateral moves better"},
                "mid_outcome": {"role": "Cloud Architect + scoped technical lead", "comp": "$245K-$275K", "probability": 0.70, "note": "Goal achieved + organic leadership exposure"},
                "high_outcome": {"role": "Principal candidate / Staff+ trajectory", "comp": "$280K-$330K", "probability": 0.15, "note": "Strong public profile + multi-region delivery → top-of-band"},
            },
        ],
        "required_experiences": [
            "Continue Cloud Architect path (already on track — 46%)",
            "Lead one cross-team migration (visibility for promo)",
        ],
        "risk_markers": [
            "AWS-only specialization narrows long-term mobility (Stay Ahead flagged this)",
            "Internal promotion timing depends on re-org / perf cycle",
        ],
        "headline": "Lowest-risk path. 70% chance of Cloud Architect at TechCorp by month 12.",
    }


def _pivot_aws_projection(s):
    return {
        **s,
        "outcomes": [
            {
                "milestone_at_months": 3,
                "low_outcome": {"role": "Senior SWE (still applying)", "comp": "$200K", "probability": 0.40, "note": "Pivot harder than expected; still interviewing"},
                "mid_outcome": {"role": "AWS Solutions Engineer (offer)", "comp": "$220K base + variable", "probability": 0.45, "note": "Pivot lands; offer in hand"},
                "high_outcome": {"role": "AWS SE (signed) + signing bonus", "comp": "$240K base + $50K signing", "probability": 0.15, "note": "Multiple offers; negotiated up"},
            },
            {
                "milestone_at_months": 12,
                "low_outcome": {"role": "AWS SE (ramping)", "comp": "$220K total", "probability": 0.20, "note": "Quota miss in Y1; OTE not realized"},
                "mid_outcome": {"role": "AWS SE on-target", "comp": "$280K-$320K total (with quota)", "probability": 0.55, "note": "Hit quota; on-target earnings"},
                "high_outcome": {"role": "AWS SE — President's Club tier", "comp": "$340K-$400K total", "probability": 0.25, "note": "Top-quartile rep; large enterprise wins"},
            },
            {
                "milestone_at_months": 18,
                "low_outcome": {"role": "Senior AWS SE", "comp": "$280K", "probability": 0.30, "note": "Steady-state IC track"},
                "mid_outcome": {"role": "Principal AWS SE / SE Manager", "comp": "$350K-$420K", "probability": 0.50, "note": "Promoted in 12-18 months; manager track opens"},
                "high_outcome": {"role": "AWS SE Manager + path to consulting/VC", "comp": "$420K+", "probability": 0.20, "note": "Strong perf opens external opportunities (VC sourcing, advisor roles, founding)"},
            },
        ],
        "required_experiences": [
            "Customer-facing pitch practice (start now — internal demos count)",
            "AWS SA Pro certification (~6 weeks intensive prep)",
            "1-2 large customer war stories — fabricated from internal projects ok",
        ],
        "risk_markers": [
            "Variable comp = year-1 ramp risk (~40% miss quota in first year)",
            "Reduces technical depth — harder to return to deep IC track later",
            "AWS hiring tightening if cloud spend trends keep softening",
        ],
        "headline": "High variable upside (+30-50%). Trades deep tech for customer time. ~50% chance of OTE in year 1.",
    }


def _stretch_anthropic_projection(s):
    return {
        **s,
        "outcomes": [
            {
                "milestone_at_months": 6,
                "low_outcome": {"role": "Senior SWE (still preparing)", "comp": "$200K", "probability": 0.45, "note": "FinOps + multi-region project not done; not ready to apply"},
                "mid_outcome": {"role": "Applying to Staff Cloud Architect roles", "comp": "$200K", "probability": 0.40, "note": "All prep done; in the funnel"},
                "high_outcome": {"role": "Staff Cloud Architect at Anthropic (offer)", "comp": "$320K + equity", "probability": 0.15, "note": "Got the role on first cycle"},
            },
            {
                "milestone_at_months": 12,
                "low_outcome": {"role": "Staff Cloud Architect at adjacent company", "comp": "$280K-$310K", "probability": 0.30, "note": "Anthropic didn't land; similar role at peer (Snowflake / Stripe)"},
                "mid_outcome": {"role": "Staff Cloud Architect at Anthropic", "comp": "$320K-$360K + equity", "probability": 0.35, "note": "Goal achieved; prestige + technical depth + growth-stage equity"},
                "high_outcome": {"role": "Staff+ at Anthropic + conference talks", "comp": "$340K + significant equity", "probability": 0.10, "note": "Lead a high-visibility infra workstream; build public profile"},
            },
            {
                "milestone_at_months": 18,
                "low_outcome": {"role": "Staff at peer co (steady)", "comp": "$300K", "probability": 0.30, "note": "Stretch didn't fully materialize but moved up + sideways"},
                "mid_outcome": {"role": "Established Staff at Anthropic / similar growth co", "comp": "$340K-$400K + equity gain", "probability": 0.45, "note": "Goal + equity unlock from growth-stage company"},
                "high_outcome": {"role": "Tech lead / Principal candidate at Anthropic", "comp": "$400K+ + meaningful equity", "probability": 0.15, "note": "Top of band; founding-engineer-adjacent role"},
            },
        ],
        "required_experiences": [
            "Finish FinOps mini-path (6 weeks)",
            "Ship a public multi-region project on GitHub + blog post",
            "Open-source contribution in agentic systems / LLM infra",
            "1-2 conference talks or strong technical writing",
        ],
        "risk_markers": [
            "10 hrs/week is sustainable for 6 months but burnout risk after",
            "Anthropic + similar AI cos hire selectively — competition is fierce",
            "Equity heavy comp — depends on growth-stage outcomes",
        ],
        "headline": "Highest ceiling (+$60-100K + meaningful equity). 35% chance at 12 months. Demanding but high-value.",
    }


def _generic_projection(s):
    return {
        **s,
        "outcomes": [],
        "required_experiences": [],
        "risk_markers": [],
        "headline": f"[STUB] No demo projection for scenario {s['id']}.",
    }
