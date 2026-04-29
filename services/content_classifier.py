"""
Content classifier — V3.

Given the title + body text of an indexable content item (Drive doc,
Confluence page, web tutorial, internal LMS course), produce structured
metadata for the content index + recommendation engine:

  {
    "summary": str (≤ 80 words),
    "skills":  list[str]  — normalized skill clusters
    "concepts_covered": list[str] — finer-grained concept names
    "prerequisites": list[str] — concepts the learner should already know
    "difficulty": "beginner" | "intermediate" | "advanced",
    "content_type": "doc" | "video" | "tutorial" | "reference" | "exercise",
    "duration_minutes_estimate": int,
    "quality_score": 0.0–1.0,
  }

Stub mode (no ANTHROPIC_API_KEY): keyword extraction + heuristics. Good
enough for the demo; live Claude is dramatically better.
"""

import os
import json


def is_live() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


def classify_content(title: str, text: str, source: str = "", source_url: str = "") -> dict:
    if is_live():
        try:
            return _classify_via_claude(title, text, source, source_url)
        except Exception as exc:
            print(f"[content_classifier] Claude failed, falling back to stub: {exc}")
    return _classify_stub(title, text, source)


# ──────────────────────────────────────────────────────────────
# Claude live mode
# ──────────────────────────────────────────────────────────────

def _classify_via_claude(title: str, text: str, source: str, source_url: str) -> dict:
    from . import claude_client

    system = (
        "You are a learning-content classifier for an enterprise learning platform. "
        "Given a content item, extract structured metadata. Return ONLY a JSON object with: "
        "summary (≤80 words, action-oriented), "
        "skills (3–6 normalized cluster names like 'kubernetes', 'aws-iam', 'data-modeling'), "
        "concepts_covered (5–15 specific concepts mentioned), "
        "prerequisites (3–8 concepts a reader needs first), "
        "difficulty ('beginner'|'intermediate'|'advanced'), "
        "content_type ('doc'|'video'|'tutorial'|'reference'|'exercise'), "
        "duration_minutes_estimate (int — reading time @ 250 wpm OR video length if video), "
        "quality_score (0.0–1.0; reward clarity, examples, structure)."
    )
    user = (
        f"TITLE: {title}\n"
        f"SOURCE: {source}\n"
        f"URL: {source_url}\n\n"
        f"TEXT:\n{(text or '')[:8000]}"
    )
    raw = claude_client._call_claude(system, [{"role": "user", "content": user}], max_tokens=800)
    parsed = claude_client._parse_json_response(raw, fallback={})
    if not parsed:
        return _classify_stub(title, text, source)
    parsed.setdefault("skills", [])
    parsed.setdefault("concepts_covered", [])
    parsed.setdefault("prerequisites", [])
    parsed.setdefault("difficulty", "intermediate")
    parsed.setdefault("content_type", "doc")
    parsed.setdefault("duration_minutes_estimate", _estimate_minutes(text))
    parsed.setdefault("quality_score", 0.7)
    parsed.setdefault("summary", title)
    parsed["_mode"] = "claude"
    return parsed


# ──────────────────────────────────────────────────────────────
# Local heuristic stub
# ──────────────────────────────────────────────────────────────

# Lightweight skill cluster vocabulary — shared with the recommender. Keep
# this list short (additions are cheap; precision matters more than recall
# at the cluster level).
SKILL_VOCAB = {
    "kubernetes": ["kubernetes", "k8s", "kubelet", "pod", "deployment", "ingress", "service mesh"],
    "aws": ["aws", "ec2", "s3", "iam", "lambda", "vpc", "cloudwatch", "cloudformation"],
    "gcp": ["gcp", "google cloud", "gke", "cloud run", "bigquery"],
    "docker": ["docker", "container", "dockerfile", "compose"],
    "terraform": ["terraform", "iac", "infrastructure as code"],
    "python": ["python", "pip", "asyncio", "pytest"],
    "javascript": ["javascript", "node", "npm", "react", "typescript"],
    "data-modeling": ["schema", "data model", "normalization", "erd"],
    "sql": ["sql", "postgres", "query", "join", "index"],
    "ml": ["machine learning", "ml", "model", "training", "neural"],
    "mlops": ["mlops", "model serving", "feature store", "drift"],
    "security": ["security", "auth", "encryption", "rbac", "tls", "mtls"],
    "networking": ["network", "tcp", "dns", "load balancer", "routing"],
    "observability": ["observability", "tracing", "metrics", "logs", "monitoring"],
    "leadership": ["leadership", "manage", "feedback", "1-on-1", "coaching"],
}

DIFFICULTY_HINTS = {
    "beginner":     ["intro", "basics", "fundamentals", "getting started", "101", "overview"],
    "intermediate": ["intermediate", "deep dive", "patterns", "best practices"],
    "advanced":     ["advanced", "expert", "internals", "performance", "production-grade", "at scale"],
}

CONTENT_TYPE_HINTS = {
    "video":     ["youtube", "video", "vimeo", ".mp4"],
    "tutorial":  ["tutorial", "step by step", "walkthrough", "lab"],
    "reference": ["reference", "api docs", "spec"],
    "exercise":  ["exercise", "kata", "practice", "homework"],
}


def _classify_stub(title: str, text: str, source: str) -> dict:
    blob = f"{title} {text}".lower()

    skills = sorted({
        cluster
        for cluster, kws in SKILL_VOCAB.items()
        if any(kw in blob for kw in kws)
    })
    concepts_covered = sorted({
        kw
        for kws in SKILL_VOCAB.values()
        for kw in kws
        if kw in blob
    })[:12]

    difficulty = "intermediate"
    for diff, hints in DIFFICULTY_HINTS.items():
        if any(h in blob for h in hints):
            difficulty = diff
            break

    content_type = "doc"
    for ctype, hints in CONTENT_TYPE_HINTS.items():
        if any(h in blob for h in hints):
            content_type = ctype
            break

    return {
        "summary": (text or title)[:240],
        "skills": skills[:6] or ["general"],
        "concepts_covered": concepts_covered or [],
        "prerequisites": _stub_prereqs(skills),
        "difficulty": difficulty,
        "content_type": content_type,
        "duration_minutes_estimate": _estimate_minutes(text),
        "quality_score": 0.6 if len(text or "") > 500 else 0.4,
        "_mode": "stub",
    }


def _stub_prereqs(skills: list) -> list:
    """Crude prereq inference: if a skill cluster has prereqs in our map, list them."""
    prereq_map = {
        "kubernetes": ["docker", "linux"],
        "service mesh": ["kubernetes", "networking"],
        "terraform": ["aws", "cloud-fundamentals"],
        "mlops": ["ml", "docker"],
        "advanced-k8s": ["kubernetes"],
    }
    out = set()
    for s in skills:
        for pre in prereq_map.get(s, []):
            out.add(pre)
    return sorted(out)


def _estimate_minutes(text: str) -> int:
    if not text:
        return 5
    words = len(text.split())
    return max(1, round(words / 250))  # 250 wpm
