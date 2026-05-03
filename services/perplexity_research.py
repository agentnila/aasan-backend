"""
Perplexity Sonar — research-mode candidate finder for the Path Engine.
======================================================================

This is the L5 replacement for the curated `content_index` catalog as the
primary candidate source for path generation. Sonar is purpose-built for
research: it browses the live web, returns structured citations, and
finds current resources without us having to hand-curate or re-validate.

Architecture:
  Path Engine flow becomes a two-step:
    1. Perplexity Sonar (THIS MODULE) → finds 20-40 real learning resources
       with live URLs and short rationales. Public web only.
    2. Claude (existing _generate_phased_path_via_claude) → organizes the
       candidates into 3-6 phases with rationale + deliverable. The phasing
       logic doesn't change — only the candidate source.

Falls through to content_catalog (Pinecone) when PERPLEXITY_API_KEY is
unset, so dev / fallback paths keep working.

Sonar tier (PERPLEXITY_SONAR_MODEL env var, defaults to sonar-pro):
  sonar              — fastest, cheapest, decent
  sonar-pro          — DEFAULT. Good citations + multi-source synthesis. ~$0.03/call
  sonar-reasoning    — Better synthesis but slower (~10-20s)
  sonar-deep-research — Best quality but 30-90s (too slow for goal-create UX)
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

logger = logging.getLogger(__name__)

PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY")
SONAR_MODEL = os.environ.get("PERPLEXITY_SONAR_MODEL", "sonar-pro")
SONAR_API_URL = "https://api.perplexity.ai/chat/completions"


def is_live() -> bool:
    """Are we configured to call Perplexity Sonar?"""
    return bool(PERPLEXITY_API_KEY)


def find_learning_candidates(
    goal_text: str,
    context_text: str = "",
    top_n: int = 30,
    timeout_s: int = 45,
) -> list[dict]:
    """
    Ask Perplexity Sonar to research current learning content for a goal.

    Returns a list of candidate dicts in the SAME shape that
    path_engine._generate_phased_path_via_claude expects:
      {
        "content_id": "<synthetic>",   # Sonar candidates aren't in our DB
        "source": "Coursera",
        "title": "...",
        "source_url": "https://...",
        "content_type": "course" | "video" | "article" | "book" | "interactive",
        "duration_minutes": 180,        # estimate
        "is_free": true,
        "difficulty": "beginner" | "intermediate" | "advanced",
        "skills": ["..."],
        "description": "Short Sonar-supplied rationale",
        "_source_engine": "perplexity_sonar",
      }
    Empty list on failure, missing key, or empty Sonar response.

    `content_id` is set to a stable string like `pplx-<sha>` so the
    phasing prompt can SELECT it; downstream materialization reads
    title/source_url/etc directly from the candidate dict (no DB lookup).
    """
    if not is_live():
        return []
    query = _compose_query(goal_text, context_text)
    if not query:
        return []
    raw = _call_sonar(query, top_n, timeout_s)
    if not raw:
        return []
    return _parse_candidates(raw, top_n)


def _compose_query(goal_text: str, context_text: str) -> str:
    """Build the Sonar prompt — a research brief, not a chat message."""
    parts = [goal_text.strip()]
    if context_text and context_text.strip():
        parts.append("Context: " + context_text.strip()[:1500])
    return "\n\n".join(p for p in parts if p)


def _call_sonar(query: str, top_n: int, timeout_s: int) -> str | None:
    """One HTTP call to Perplexity Sonar. Returns raw response text or None."""
    import requests

    system_prompt = (
        "You are a learning-resource researcher. Given a learner's goal, "
        "find the BEST current learning resources for it across the public web "
        "— Coursera, Udemy, edX, DeepLearning.AI, YouTube, official docs, "
        "university programs, books, podcasts, blogs, etc. Mix free and paid.\n\n"
        f"Return EXACTLY {top_n} resources that span foundations → specialization "
        "→ application/capstone. Prefer recent (2024-2026) content. Prefer "
        "high-signal authors (Anthropic / Andrew Ng / official docs / named "
        "instructors at reputable institutions).\n\n"
        "OUTPUT FORMAT — return ONLY a JSON object, no markdown fences, no prose:\n"
        "{\n"
        '  "candidates": [\n'
        '    {\n'
        '      "title":            "Short course / resource title",\n'
        '      "source":           "Coursera | Udemy | DeepLearning.AI | YouTube | etc.",\n'
        '      "source_url":       "https://... — must be a real, current URL",\n'
        '      "content_type":     "course | video | article | book | interactive | lab",\n'
        '      "duration_minutes": 180,\n'
        '      "is_free":          true,\n'
        '      "difficulty":       "beginner | intermediate | advanced",\n'
        '      "skills":           ["langchain", "rag", "tool-use"],\n'
        '      "description":      "1 sentence — why this resource is relevant to the goal"\n'
        '    }\n'
        '  ]\n'
        "}\n\n"
        "RULES:\n"
        "  - source_url must be a real, current URL (you have web access — verify if uncertain)\n"
        "  - duration_minutes is a realistic estimate (course total time, not video length)\n"
        "  - skills are short lowercase tags relevant to the goal (3-6 per item)\n"
        "  - description should explain WHY this resource fits the learner's goal\n"
        "  - Span foundations → application; don't cluster everything at one level\n"
        "  - Mix sources; don't return 30 Coursera links if Udemy/YouTube/docs would serve better"
    )

    body = {
        "model": SONAR_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Learner goal:\n\n{query}"},
        ],
        "temperature": 0.2,
    }

    try:
        response = requests.post(
            SONAR_API_URL,
            headers={
                "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=timeout_s,
        )
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices") or []
        if not choices:
            logger.warning("Sonar returned no choices")
            return None
        msg = choices[0].get("message") or {}
        return msg.get("content") or None
    except requests.HTTPError as exc:
        body_snippet = exc.response.text[:200] if exc.response is not None else ""
        logger.warning("Sonar HTTP error: %s · %s", exc, body_snippet)
        return None
    except Exception as exc:
        logger.warning("Sonar call failed (%s)", exc)
        return None


def _parse_candidates(raw: str, top_n: int) -> list[dict]:
    """Extract the JSON candidates list from Sonar's response."""
    text = raw.strip()
    # Strip markdown fences if present despite our prompt
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```\s*$", "", text)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        # Sometimes Sonar adds preamble before the JSON. Find the first `{` and last `}`.
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            logger.warning("Could not find JSON object in Sonar response")
            return []
        try:
            parsed = json.loads(text[start:end + 1])
        except json.JSONDecodeError as exc:
            logger.warning("Sonar JSON parse failed: %s", exc)
            return []

    raw_items = parsed.get("candidates") if isinstance(parsed, dict) else None
    if not isinstance(raw_items, list):
        logger.warning("Sonar response missing candidates list")
        return []

    out: list[dict] = []
    for i, c in enumerate(raw_items[:top_n]):
        if not isinstance(c, dict):
            continue
        title = (c.get("title") or "").strip()
        url = (c.get("source_url") or c.get("url") or "").strip()
        if not title or not url:
            continue
        if not (url.startswith("http://") or url.startswith("https://")):
            continue

        skills_raw = c.get("skills")
        if isinstance(skills_raw, str):
            skills = [s.strip().lower() for s in skills_raw.split(",") if s.strip()]
        elif isinstance(skills_raw, list):
            skills = [str(s).strip().lower() for s in skills_raw if str(s).strip()]
        else:
            skills = []

        try:
            duration = int(c.get("duration_minutes") or 60) or 60
        except (TypeError, ValueError):
            duration = 60

        difficulty = (c.get("difficulty") or "").lower().strip()
        if difficulty not in ("beginner", "intermediate", "advanced", "expert"):
            difficulty = None

        ctype = (c.get("content_type") or "course").lower().strip()
        if ctype not in ("course", "video", "article", "lab", "quiz", "pdf",
                         "book", "slides", "interactive", "other"):
            ctype = "course"

        out.append({
            # Synthetic ID — Claude SELECTs by this in the phasing prompt.
            # Format: "pplx-N" so it's distinguishable from "content-N" catalog IDs.
            "content_id": f"pplx-{i + 1}",
            "source": (c.get("source") or "Web").strip()[:60],
            "title": title[:200],
            "source_url": url,
            "content_type": ctype,
            "duration_minutes": duration,
            "is_free": bool(c.get("is_free")) if c.get("is_free") is not None else True,
            "difficulty": difficulty,
            "skills": skills[:8],
            "description": (c.get("description") or "").strip()[:300] or None,
            "_source_engine": "perplexity_sonar",
        })
    return out


def diag() -> dict:
    """Diagnostic — call site for /diag/research_status."""
    return {
        "is_live": is_live(),
        "model": SONAR_MODEL,
        "endpoint": SONAR_API_URL,
    }
