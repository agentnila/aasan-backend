"""
Content pre-digestion orchestration — third Perplexity Computer use case.

WHEN PERAASAN PRE-DIGESTS
─────────────────────────
The learner pastes a long doc URL (a research paper, a 40-page Confluence runbook,
a vendor whitepaper, a long blog post) and asks for the substance without reading
the whole thing.

PIPELINE
────────
  Perplexity Computer  →  fetches the page deeply (handles pagination, follows
                          related links, extracts the substantive body — much
                          richer than a one-shot fetch_url)
  Claude               →  extracts structured digest:
                          - title, source
                          - TL;DR (1-2 sentences)
                          - 5 key concepts (name + 1-line body + importance score)
                          - reading time saved
                          - suggested next step (tied to learner's goals if known)

Why Perplexity Computer for the fetch (not Bridge):
  - Long docs need follow-link navigation (footnotes, references, multi-page TOCs)
  - Pre-digest runs OUTSIDE the learner's browser context (background, server-side)
  - Result is cacheable — same URL → same digest (within freshness window)

STUB MODE
─────────
Without PERPLEXITY_API_KEY, returns a believable digest that incorporates the URL
so the demo loop is observable.
"""

from datetime import datetime
from . import perplexity_client, claude_client


def predigest(url: str, learner_context: dict = None) -> dict:
    """
    Pre-digest a single URL. Returns a structured digest ready for chat surfacing.

    Returns: {
      url, title, source_domain,
      tldr,
      key_concepts: [{ name, body, importance: 0.0-1.0 }, ...],
      reading_time_saved_minutes,
      suggested_next_step,
      modes: { computer, classifier },
      fetched_at,
    }
    """
    if not url:
        return _error_response("URL is required")

    learner_context = learner_context or {}

    # 1. Deep fetch via Perplexity Computer
    fetch = perplexity_client.fetch_url(url)
    if fetch.get("status") != "ok":
        return _error_response(f"Could not fetch URL: {fetch.get('error', {}).get('message', 'unknown')}")

    fetched = fetch.get("result", {})
    main_text = fetched.get("main_text", "")
    title = fetched.get("title", url)
    source_domain = _extract_domain(url)

    # 2. Stub fast-path — when Computer is in stub mode, produce a believable
    #    digest tailored to the URL so the demo is coherent
    if not perplexity_client.is_live():
        return _stub_digest(url, title, source_domain, learner_context)

    # 3. Real digest — Claude extracts structured concepts + TL;DR
    digest = claude_client.extract_concepts(
        conversation_text=f"Title: {title}\n\nContent:\n{main_text}",
        is_page_read=True,  # Mastery 0.2 — learner was exposed, didn't actively study
    )

    concepts = digest.get("concepts", [])[:5]
    return {
        "url": url,
        "title": title,
        "source_domain": source_domain,
        "tldr": digest.get("summary") or _synthesize_tldr_fallback(main_text),
        "key_concepts": [
            {
                "name": c.get("name"),
                "body": c.get("definition", "")[:200],
                "importance": c.get("confidence", 0.5),
            }
            for c in concepts
        ],
        "reading_time_saved_minutes": _estimate_reading_time(main_text),
        "suggested_next_step": _suggest_next_step(concepts, learner_context),
        "modes": {
            "computer": "live" if perplexity_client.is_live() else "stub",
            "classifier": "live" if claude_client.is_live() else "stub",
        },
        "fetched_at": fetched.get("fetched_at", datetime.utcnow().isoformat()),
    }


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _extract_domain(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc or url
    except Exception:
        return url


def _estimate_reading_time(text: str) -> int:
    """Roughly: 250 words/minute reading speed."""
    word_count = len(text.split())
    return max(1, round(word_count / 250))


def _synthesize_tldr_fallback(text: str) -> str:
    """If Claude returned no summary, take the first 2 sentences as a fallback."""
    sentences = text.replace("\n", " ").split(". ")
    return ". ".join(sentences[:2])[:300] + ("." if not text.endswith(".") else "")


def _suggest_next_step(concepts: list, learner_context: dict) -> str:
    """Lightweight: tie to learner goal if we know it. Real impl uses Claude reasoning."""
    goal = learner_context.get("goal")
    if goal and concepts:
        first = concepts[0].get("name", "the first concept")
        return f"Want to do a 5-min deep-dive on {first}? It connects directly to your goal: {goal}."
    if concepts:
        return f"Want a quick recap quiz on the {len(concepts)} key concepts to lock them in?"
    return "Want me to find related content based on what you just read?"


def _error_response(message: str) -> dict:
    return {
        "error": message,
        "modes": {
            "computer": "live" if perplexity_client.is_live() else "stub",
            "classifier": "live" if claude_client.is_live() else "stub",
        },
    }


# ──────────────────────────────────────────────────────────────
# Stub digest — believable result for any URL
# ──────────────────────────────────────────────────────────────

def _stub_digest(url: str, title: str, source_domain: str, learner_context: dict) -> dict:
    """
    Deterministic stub that incorporates the URL. Returns a coherent
    "5 concept" digest with reasoning so the demo tells a story even
    in stub mode.
    """
    # Tailor a few example digests to common-looking URLs so the demo is convincing
    lower_url = url.lower()

    if "kubernetes" in lower_url or "k8s" in lower_url:
        return _stub_kubernetes_digest(url, title, source_domain, learner_context)
    if "lambda" in lower_url or "aws" in lower_url:
        return _stub_aws_digest(url, title, source_domain, learner_context)
    if "anthropic" in lower_url or "claude" in lower_url:
        return _stub_anthropic_digest(url, title, source_domain, learner_context)

    # Generic believable digest
    return {
        "url": url,
        "title": title or "[STUB] Pre-digested document",
        "source_domain": source_domain,
        "tldr": (
            f"[STUB] Perplexity Computer is not configured. With PERPLEXITY_API_KEY set, "
            f"this would be a real deep-read of {url} with Claude-synthesized 5-concept "
            f"digest tailored to your goal."
        ),
        "key_concepts": [
            {"name": "Concept 1 from the source", "body": "First key idea — extracted from main body.", "importance": 0.9},
            {"name": "Concept 2", "body": "Second key idea — supports concept 1.", "importance": 0.8},
            {"name": "Concept 3", "body": "Third key idea — practical implication.", "importance": 0.7},
            {"name": "Concept 4", "body": "Fourth key idea — common misconception clarified.", "importance": 0.6},
            {"name": "Concept 5", "body": "Fifth key idea — reference for further reading.", "importance": 0.5},
        ],
        "reading_time_saved_minutes": 18,
        "suggested_next_step": _suggest_next_step([{"name": "Concept 1 from the source"}], learner_context),
        "modes": {"computer": "stub", "classifier": "stub"},
        "fetched_at": datetime.utcnow().isoformat(),
        "_stub": True,
    }


def _stub_kubernetes_digest(url, title, source_domain, learner_context):
    return {
        "url": url,
        "title": title or "Kubernetes deep-read",
        "source_domain": source_domain,
        "tldr": (
            "Long-form Kubernetes content — covers Pods, Services, Deployments, networking, and "
            "operational patterns. Most relevant for engineers operating production clusters."
        ),
        "key_concepts": [
            {"name": "Pod lifecycle and scheduling", "body": "How the scheduler places pods on nodes; node selectors, taints, tolerations, and affinity rules.", "importance": 0.92},
            {"name": "Service types & networking", "body": "ClusterIP, NodePort, LoadBalancer, ExternalName — when to use each. kube-proxy implements routing via iptables/ipvs.", "importance": 0.88},
            {"name": "Deployments vs StatefulSets", "body": "Deployments for stateless replicas with rolling updates; StatefulSets for ordered, persistent identity.", "importance": 0.85},
            {"name": "ConfigMaps & Secrets", "body": "Config injection patterns. Secrets are base64-encoded by default — encryption at rest needs explicit setup.", "importance": 0.78},
            {"name": "Resource limits & QoS classes", "body": "requests vs limits → BestEffort / Burstable / Guaranteed. Wrong settings cause OOMKills under pressure.", "importance": 0.72},
        ],
        "reading_time_saved_minutes": 24,
        "suggested_next_step": _suggest_next_step(
            [{"name": "Service types & networking"}], learner_context,
        ),
        "modes": {"computer": "stub", "classifier": "stub"},
        "fetched_at": datetime.utcnow().isoformat(),
        "_stub": True,
    }


def _stub_aws_digest(url, title, source_domain, learner_context):
    return {
        "url": url,
        "title": title or "AWS deep-read",
        "source_domain": source_domain,
        "tldr": (
            "AWS service documentation — covers core service capabilities, pricing model, "
            "common architectural patterns, and notable limits. Includes runtime/version "
            "deprecation timeline."
        ),
        "key_concepts": [
            {"name": "Service capability summary", "body": "What the service does, its primary use cases, and where it fits in a cloud architecture.", "importance": 0.9},
            {"name": "Runtime/version timeline", "body": "Currently supported versions, deprecation notices (nodejs16.x deprecated; nodejs20.x is now default).", "importance": 0.92},
            {"name": "IAM model & least-privilege patterns", "body": "Resource policies vs identity policies; common patterns for cross-account access.", "importance": 0.82},
            {"name": "Cost model & optimization", "body": "How pricing scales with usage; common cost-driver gotchas; cost optimization patterns.", "importance": 0.78},
            {"name": "Quotas & limits", "body": "Service-level limits that bite at scale; how to request quota increases.", "importance": 0.65},
        ],
        "reading_time_saved_minutes": 19,
        "suggested_next_step": _suggest_next_step(
            [{"name": "Runtime/version timeline"}], learner_context,
        ),
        "modes": {"computer": "stub", "classifier": "stub"},
        "fetched_at": datetime.utcnow().isoformat(),
        "_stub": True,
    }


def _stub_anthropic_digest(url, title, source_domain, learner_context):
    return {
        "url": url,
        "title": title or "Anthropic / Claude deep-read",
        "source_domain": source_domain,
        "tldr": (
            "Anthropic guidance on building with Claude — covers prompting patterns, tool use, "
            "agentic workflows, and best practices for production deployments."
        ),
        "key_concepts": [
            {"name": "Prompt structure best practices", "body": "System prompt + user message conventions; XML tags for structured input; chain-of-thought with thinking blocks.", "importance": 0.88},
            {"name": "Tool use / function calling", "body": "Define tools as JSON schemas; Claude returns structured tool_use blocks; you execute and return tool_result blocks.", "importance": 0.92},
            {"name": "Agentic loops", "body": "Multi-step plan-act-observe loops; explicit reasoning blocks; managing long-running conversations.", "importance": 0.85},
            {"name": "Prompt caching", "body": "Mark portions of long prompts as cacheable; 90% cost reduction on cache hits; 5-minute TTL.", "importance": 0.8},
            {"name": "Evaluation & guardrails", "body": "Building evals for production; safety guardrails; red-teaming patterns.", "importance": 0.7},
        ],
        "reading_time_saved_minutes": 22,
        "suggested_next_step": _suggest_next_step(
            [{"name": "Tool use / function calling"}], learner_context,
        ),
        "modes": {"computer": "stub", "classifier": "stub"},
        "fetched_at": datetime.utcnow().isoformat(),
        "_stub": True,
    }
