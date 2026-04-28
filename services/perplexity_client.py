"""
Perplexity Computer client — V3 deep agentic layer.

DESIGN INTENT
─────────────
Perplexity Computer is the named server-side agentic option for V3. It handles
the deep web research + automation workloads:
  - Currency Watch (re-fetch concept sources, daily)
  - Career Compass market scrape (job-board postings, weekly)
  - Course-launch monitoring (vendor portals, daily)
  - Content pre-digestion (long docs / videos -> key concepts)
  - Course enrollment automations (multi-step web flows)

This module is the ONLY place that knows how to talk to Perplexity Computer.
Every consumer (currency, career, content, enrollment) calls through the
generic `run_task()` entry point with a structured task spec.

STUB MODE
─────────
When PERPLEXITY_API_KEY is unset (default in dev / when Balaji's MacMini is
unreachable), this module returns deterministic mock responses that match the
real API shape. This lets us build and test the full pipeline now and flip to
real calls by setting one env var.

API SHAPE (placeholder until we have the official docs)
───────────────────────────────────────────────────────
Every task is a dict:
    {
      "kind": "fetch_url" | "scrape_pattern" | "watch_changes" | "research" | "enroll",
      "input": <kind-specific dict>,
      "constraints": {"timeout_s": int, "max_pages": int, ...}
    }

Every result is a dict:
    {
      "status": "ok" | "error" | "partial" | "not_connected",
      "result": <kind-specific dict>,
      "metadata": {"task_id": str, "duration_ms": int, "cost_estimate_usd": float}
    }

Real HTTP wiring is in `_call_perplexity_computer()`. Replace the stub
response logic in `_stub_response()` once the official API is integrated.
"""

import os
import json
import time
import hashlib
import requests
from datetime import datetime

PERPLEXITY_API_KEY = os.environ.get("PERPLEXITY_API_KEY")
PERPLEXITY_API_URL = os.environ.get("PERPLEXITY_API_URL", "https://api.perplexity.ai/computer/v1/tasks")
DEFAULT_TIMEOUT_S = 60


def is_live():
    """True if we have credentials. False means stub mode."""
    return bool(PERPLEXITY_API_KEY)


def run_task(task: dict, timeout_s: int = DEFAULT_TIMEOUT_S) -> dict:
    """
    Generic entry point. Every consumer calls this.
    Returns a dict with status / result / metadata.

    In stub mode: returns deterministic mock responses.
    In live mode: hits the real Perplexity Computer API.
    """
    started = time.time()
    task_id = _task_id(task)

    if not is_live():
        result = _stub_response(task)
        return {
            "status": result["status"],
            "result": result["result"],
            "metadata": {
                "task_id": task_id,
                "duration_ms": int((time.time() - started) * 1000),
                "mode": "stub",
                "cost_estimate_usd": 0.0,
            },
        }

    try:
        result = _call_perplexity_computer(task, timeout_s)
        return {
            "status": "ok",
            "result": result,
            "metadata": {
                "task_id": task_id,
                "duration_ms": int((time.time() - started) * 1000),
                "mode": "live",
                # Cost estimate placeholder until we know real pricing
                "cost_estimate_usd": _estimate_cost(task),
            },
        }
    except requests.exceptions.Timeout:
        return _error_response(task_id, started, "timeout", f"Task timed out after {timeout_s}s")
    except requests.exceptions.RequestException as e:
        return _error_response(task_id, started, "http_error", str(e))
    except Exception as e:
        return _error_response(task_id, started, "internal_error", str(e))


# ──────────────────────────────────────────────────────────────
# Convenience wrappers — typed task constructors per kind
# Each consumer should use these so task shapes stay consistent.
# ──────────────────────────────────────────────────────────────

def fetch_url(url: str, timeout_s: int = 30) -> dict:
    """Fetch a single URL and return its main text content."""
    return run_task({
        "kind": "fetch_url",
        "input": {"url": url},
        "constraints": {"timeout_s": timeout_s, "max_chars": 8000},
    }, timeout_s=timeout_s)


def watch_changes(url: str, baseline_hash: str = None) -> dict:
    """Fetch a URL and report whether it changed vs the baseline content hash."""
    return run_task({
        "kind": "watch_changes",
        "input": {"url": url, "baseline_hash": baseline_hash},
        "constraints": {"timeout_s": 30},
    })


def scrape_pattern(query: str, sources: list, max_results: int = 50) -> dict:
    """
    Multi-source scrape — used by Career Compass market scan.
    sources: list of base URLs or search-engine queries.
    """
    return run_task({
        "kind": "scrape_pattern",
        "input": {"query": query, "sources": sources, "max_results": max_results},
        "constraints": {"timeout_s": 300, "max_pages": max_results},
    }, timeout_s=300)


def research(question: str, depth: str = "medium") -> dict:
    """Deep multi-step web research."""
    return run_task({
        "kind": "research",
        "input": {"question": question, "depth": depth},
        "constraints": {"timeout_s": 180},
    }, timeout_s=180)


def enroll_in_course(course_url: str, credentials_ref: str) -> dict:
    """Drive an enrollment flow on the learner's behalf."""
    return run_task({
        "kind": "enroll",
        "input": {"course_url": course_url, "credentials_ref": credentials_ref},
        "constraints": {"timeout_s": 120},
    }, timeout_s=120)


# ──────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────

def _task_id(task: dict) -> str:
    """Stable ID from task content — useful for de-duping + logging."""
    payload = json.dumps(task, sort_keys=True).encode("utf-8")
    return f"task-{hashlib.sha256(payload).hexdigest()[:12]}"


def _call_perplexity_computer(task: dict, timeout_s: int) -> dict:
    """Real HTTP call. Endpoint shape is provisional until we have official docs."""
    response = requests.post(
        PERPLEXITY_API_URL,
        headers={
            "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
            "Content-Type": "application/json",
        },
        json=task,
        timeout=timeout_s,
    )
    response.raise_for_status()
    return response.json()


def _estimate_cost(task: dict) -> float:
    """Rough cost guess — replace with real pricing once known."""
    base = {
        "fetch_url": 0.005,
        "watch_changes": 0.005,
        "scrape_pattern": 0.10,
        "research": 0.05,
        "enroll": 0.02,
    }.get(task.get("kind"), 0.01)
    return base


def _error_response(task_id, started, code, message):
    return {
        "status": "error",
        "error": {"code": code, "message": message},
        "metadata": {
            "task_id": task_id,
            "duration_ms": int((time.time() - started) * 1000),
            "mode": "live",
        },
    }


# ──────────────────────────────────────────────────────────────
# Stub responses (used until PERPLEXITY_API_KEY is set)
# Shape MUST match what the real API will return — that's the
# contract we're building against.
# ──────────────────────────────────────────────────────────────

def _stub_response(task: dict) -> dict:
    kind = task.get("kind")
    inp = task.get("input", {})

    if kind == "fetch_url":
        url = inp.get("url", "")
        return {
            "status": "ok",
            "result": {
                "url": url,
                "title": f"[STUB] Page at {url}",
                "main_text": (
                    "[STUB] Perplexity Computer is not configured (PERPLEXITY_API_KEY unset). "
                    "This is a deterministic mock response so the rest of the pipeline can be tested. "
                    "The real call will return the page's main content (~8K chars cap), "
                    "with kind='fetch_url' returning {url, title, main_text, fetched_at, content_hash}."
                ),
                "fetched_at": datetime.utcnow().isoformat(),
                "content_hash": hashlib.sha256(url.encode()).hexdigest(),
            },
        }

    if kind == "watch_changes":
        url = inp.get("url", "")
        baseline = inp.get("baseline_hash") or ""
        # Stub deterministically reports "changed" iff baseline differs from URL hash
        current_hash = hashlib.sha256(url.encode()).hexdigest()
        return {
            "status": "ok",
            "result": {
                "url": url,
                "changed": baseline != current_hash,
                "baseline_hash": baseline,
                "current_hash": current_hash,
                "main_text": f"[STUB] Current content of {url}.",
                "fetched_at": datetime.utcnow().isoformat(),
            },
        }

    if kind == "scrape_pattern":
        return {
            "status": "ok",
            "result": {
                "query": inp.get("query"),
                "sources_attempted": inp.get("sources", []),
                "results": [
                    {
                        "url": "https://example.com/sample-1",
                        "title": "[STUB] Sample result 1",
                        "snippet": "Stub snippet — wire to real Perplexity Computer for actual results.",
                    },
                    {
                        "url": "https://example.com/sample-2",
                        "title": "[STUB] Sample result 2",
                        "snippet": "Stub snippet 2.",
                    },
                ],
                "scraped_at": datetime.utcnow().isoformat(),
            },
        }

    if kind == "research":
        return {
            "status": "ok",
            "result": {
                "question": inp.get("question"),
                "answer": (
                    "[STUB] Perplexity Computer would synthesize a multi-source answer here. "
                    "Set PERPLEXITY_API_KEY to enable real research."
                ),
                "citations": [
                    {"url": "https://example.com/cite-1", "title": "[STUB] Citation 1"},
                ],
                "researched_at": datetime.utcnow().isoformat(),
            },
        }

    if kind == "enroll":
        return {
            "status": "ok",
            "result": {
                "course_url": inp.get("course_url"),
                "enrollment_status": "stubbed",
                "confirmation_id": f"STUB-{int(time.time())}",
                "message": "[STUB] Real enrollment requires PERPLEXITY_API_KEY + valid credentials_ref.",
            },
        }

    return {
        "status": "error",
        "result": {"error": f"Unknown task kind: {kind}"},
    }
