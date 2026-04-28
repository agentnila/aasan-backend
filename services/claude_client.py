"""
Claude (Anthropic) client — V3 reasoning brain.

DESIGN INTENT
─────────────
Claude is the reasoning brain in the V3 two-vendor split:
  - Chat dialogue (Sonnet, called direct from browser — not this module)
  - Substance classifier (this module — Currency Watch + Doc Change)
  - Concept extraction (Haiku — capture sessions)
  - Recommendation scoring (Sonnet — 8-dimension)
  - Path Adjustment Engine reasoning (Sonnet)

This module wraps server-side Claude calls only. Browser-side chat continues
to call the Anthropic API directly from React (see ChatPanel.jsx).

STUB MODE
─────────
When ANTHROPIC_API_KEY is unset, returns deterministic mock responses
matching the real shape. Same pattern as perplexity_client.
"""

import os
import json
import requests

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MODEL = "claude-sonnet-4-5"
HAIKU_MODEL = "claude-haiku-4-5-20251001"


def is_live():
    return bool(ANTHROPIC_API_KEY)


# ──────────────────────────────────────────────────────────────
# Substance classifier — used by /freshness/check + doc-change watcher
# ──────────────────────────────────────────────────────────────

def classify_change(old_text: str, new_text: str, context: dict = None) -> dict:
    """
    Compare old vs new text. Categorize the change:
      cosmetic / clarification / substantive / breaking

    Returns: {
      "category": str,
      "summary": str (one-line description of what materially changed),
      "affected_concepts": list[str] (best-effort),
      "confidence": float
    }

    The classifier is conservative — when in doubt, treat as cosmetic.
    Only "substantive" or "breaking" should trigger a notification.
    """
    if not is_live():
        return _stub_classification(old_text, new_text)

    context = context or {}
    system_prompt = (
        "You are a strict change-substance classifier. Given an old version of a document and a new "
        "version, categorize the change as one of: cosmetic / clarification / substantive / breaking.\n\n"
        "Rules:\n"
        "  - cosmetic: typos, formatting, link fixes, image swaps, whitespace only\n"
        "  - clarification: same meaning, better wording or examples\n"
        "  - substantive: facts changed, procedures changed, owners/thresholds changed, recommended approaches changed\n"
        "  - breaking: previously-correct guidance is now wrong; APIs/flags/paths no longer exist\n\n"
        "BE CONSERVATIVE. When in doubt, treat as cosmetic. Only substantive or breaking should generate a notification.\n"
        "Return ONLY a JSON object: "
        "{\"category\": \"cosmetic|clarification|substantive|breaking\", "
        "\"summary\": \"one-line description of what materially changed\", "
        "\"affected_concepts\": [\"concept1\", ...], "
        "\"confidence\": 0.0-1.0}"
    )

    user_prompt = (
        f"Context: {json.dumps(context)}\n\n"
        f"OLD VERSION:\n{old_text[:6000]}\n\n"
        f"NEW VERSION:\n{new_text[:6000]}"
    )

    response = _call_claude(
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
        max_tokens=512,
        model=DEFAULT_MODEL,
    )
    return _parse_json_response(response, fallback={
        "category": "cosmetic",
        "summary": "Could not parse classifier output; defaulting to cosmetic (safe).",
        "affected_concepts": [],
        "confidence": 0.0,
    })


# ──────────────────────────────────────────────────────────────
# Concept extractor — used by /capture/session
# (already has a working implementation in app.py via direct Claude call;
#  this is the future home for that logic)
# ──────────────────────────────────────────────────────────────

def extract_concepts(conversation_text: str, is_page_read: bool = False) -> dict:
    """
    Extract concepts + gaps + summary from a learning conversation.
    Returns: { concepts: [...], summary: str }
    """
    if not is_live():
        return _stub_extraction(conversation_text, is_page_read)

    max_confidence = 0.2 if is_page_read else 0.7
    exposure_note = (
        f"\nIMPORTANT: This content was READ BY THE AI AGENT (Peraasan), not actively studied "
        f"by the employee. Set confidence to at most {max_confidence} for all concepts — "
        f"this is exposure, not mastery. The employee needs to engage (ask questions, take a "
        f"quiz, apply the knowledge) before mastery increases."
        if is_page_read else ""
    )

    system_prompt = (
        "Extract key concepts from this learning conversation. "
        "Return JSON: { concepts: [{ name, definition, subject, domain, confidence, "
        "is_gap, gap_type, connects_to }], summary: string }. "
        "If this is NOT a learning conversation (just casual chat, greetings, etc.), "
        "return { concepts: [], summary: null }."
        + exposure_note
    )

    response = _call_claude(
        system=system_prompt,
        messages=[{"role": "user", "content": conversation_text[:8000]}],
        max_tokens=1024,
        model=HAIKU_MODEL,
    )
    return _parse_json_response(response, fallback={"concepts": [], "summary": None})


# ──────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────

def _call_claude(system: str, messages: list, max_tokens: int = 1024, model: str = DEFAULT_MODEL) -> str:
    """Single point that talks to the Anthropic API. Returns the text content."""
    response = requests.post(
        ANTHROPIC_API_URL,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
        },
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    blocks = data.get("content", [])
    if not blocks:
        return ""
    return blocks[0].get("text", "")


def _parse_json_response(text: str, fallback: dict) -> dict:
    """Claude sometimes wraps JSON in ```json ... ```. Strip + parse."""
    if not text:
        return fallback
    # Strip code fences
    cleaned = text.strip()
    if cleaned.startswith("```"):
        # Drop first line (```json or ```) and last line (```)
        lines = cleaned.splitlines()
        if len(lines) >= 2:
            cleaned = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to find a JSON object substring
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(cleaned[start:end + 1])
            except json.JSONDecodeError:
                pass
        return fallback


# ──────────────────────────────────────────────────────────────
# Stub responses
# ──────────────────────────────────────────────────────────────

def _stub_classification(old_text: str, new_text: str) -> dict:
    """Heuristic: char-diff size predicts category. Conservative defaults."""
    old_len = len(old_text)
    new_len = len(new_text)
    delta = abs(new_len - old_len)
    delta_pct = (delta / max(old_len, 1)) * 100

    if delta_pct < 1:
        category = "cosmetic"
        summary = "[STUB] Tiny diff (<1% size change) — almost certainly cosmetic."
    elif delta_pct < 5:
        category = "clarification"
        summary = "[STUB] Small diff (1-5%) — likely a clarification or rewording."
    elif delta_pct < 20:
        category = "substantive"
        summary = "[STUB] Moderate diff (5-20%) — flagging as substantive (real classifier needed)."
    else:
        category = "breaking"
        summary = "[STUB] Large diff (>20%) — likely breaking (real classifier needed)."

    return {
        "category": category,
        "summary": summary,
        "affected_concepts": [],
        "confidence": 0.5,
        "_stub": True,
    }


def _stub_extraction(text: str, is_page_read: bool) -> dict:
    return {
        "concepts": [],
        "summary": "[STUB] Anthropic API not configured (ANTHROPIC_API_KEY unset).",
        "_stub": True,
    }
