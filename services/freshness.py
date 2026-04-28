"""
Currency Watch orchestration.

Walks the set of "tracked concepts" — concepts the learner has captured that
have a source_url — and runs the freshness pipeline on each:

    re-fetch via Perplexity Computer
      → diff via Claude substance classifier
      → categorize: cosmetic / clarification / substantive / breaking
      → only substantive + breaking yield notifications

This module is the orchestrator. The actual fetch lives in
services.perplexity_client; the actual classifier lives in services.claude_client.

PHASE 1 STORAGE
───────────────
For Phase 1 we use a hardcoded set of demo tracked concepts (DEMO_CONCEPTS).
When Airtable is wired, this module reads from `Concept` nodes in Neo4j
(or the Concept_Index in Airtable, depending on where the source_url lives).
The function `_get_tracked_concepts(user_id)` is the swap point.
"""

from datetime import datetime
from . import perplexity_client, claude_client


# Hardcoded demo tracked concepts — replace with Neo4j query in Phase 2.
# Each entry has the fields a real Concept node would expose.
DEMO_CONCEPTS = [
    {
        "concept_name": "Kubernetes Service topology",
        "source_url": "https://kubernetes.io/docs/concepts/services-networking/service/",
        "captured_at": "2026-04-22",
        "baseline_text": (
            "A Service in Kubernetes provides a stable network endpoint for a set of Pods. "
            "Service types: ClusterIP, NodePort, LoadBalancer, ExternalName. "
            "Pods are selected via labels. The topologyKeys field controls how traffic is "
            "distributed across topology domains (zone, region) — entries are evaluated in order."
        ),
        "baseline_hash": "k8s-services-2026-04-22",
        "domain": "Cloud Infrastructure",
    },
    {
        "concept_name": "AWS Lambda runtimes",
        "source_url": "https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html",
        "captured_at": "2026-04-15",
        "baseline_text": (
            "AWS Lambda supports several runtimes including Node.js (16.x, 18.x, 20.x), "
            "Python (3.9, 3.10, 3.11, 3.12), Java (11, 17, 21), .NET (6, 8), Ruby (3.2, 3.3), "
            "Go (provided.al2). The runtime determines the language version your function executes with."
        ),
        "baseline_hash": "aws-lambda-runtimes-2026-04-15",
        "domain": "Cloud Infrastructure",
    },
    {
        "concept_name": "React Server Components",
        "source_url": "https://react.dev/reference/rsc/server-components",
        "captured_at": "2026-04-10",
        "baseline_text": (
            "Server Components run on the server and stream their rendered output to the client. "
            "They cannot use state or effects. They reduce the client JS bundle and let you fetch "
            "data without prop drilling. Marked by adding 'use server' or by being inside a server-only "
            "directory in supported frameworks."
        ),
        "baseline_hash": "react-rsc-2026-04-10",
        "domain": "Frontend",
    },
]


def get_tracked_concepts(user_id: str = None, limit: int = 5):
    """
    Return concepts due for freshness checks. Phase 1: demo set.
    Phase 2: query Neo4j for concepts owned by user_id with source_url set,
    ordered by last_freshness_check ASC NULLS FIRST.
    """
    return DEMO_CONCEPTS[:limit]


def scan_concept(concept: dict) -> dict:
    """
    Run the full freshness pipeline on one concept.

    Returns a verdict dict ready to persist + surface:
      {
        concept_name, source_url, captured_at,
        changed, category, summary, affected_concepts, confidence,
        should_notify, current_hash, fetched_at,
        metadata: { computer, classifier }
      }
    """
    # 1. Re-fetch the source via Perplexity Computer
    fetch = perplexity_client.fetch_url(concept["source_url"])

    if fetch.get("status") != "ok":
        return {
            "concept_name": concept["concept_name"],
            "source_url": concept["source_url"],
            "captured_at": concept.get("captured_at"),
            "changed": False,
            "category": "error",
            "summary": "Could not re-fetch source.",
            "should_notify": False,
            "metadata": {"computer": fetch},
        }

    fetched = fetch.get("result", {})
    current_text = fetched.get("main_text", "")
    current_hash = fetched.get("content_hash", "")
    baseline_hash = concept.get("baseline_hash", "")

    # 2. Cheap diff — short-circuit if hash matches
    if baseline_hash and baseline_hash == current_hash:
        return {
            "concept_name": concept["concept_name"],
            "source_url": concept["source_url"],
            "captured_at": concept.get("captured_at"),
            "changed": False,
            "category": "cosmetic",
            "summary": "No change detected (content hash matches baseline).",
            "should_notify": False,
            "current_hash": current_hash,
            "fetched_at": fetched.get("fetched_at"),
            "metadata": {
                "computer": fetch.get("metadata", {}),
                "classifier": {"skipped": "hash_match"},
            },
        }

    # 3. Substance classifier
    classification = claude_client.classify_change(
        old_text=concept.get("baseline_text", ""),
        new_text=current_text,
        context={
            "concept_name": concept["concept_name"],
            "captured_at": concept.get("captured_at"),
            "domain": concept.get("domain"),
        },
    )

    category = classification.get("category", "cosmetic")
    return {
        "concept_name": concept["concept_name"],
        "source_url": concept["source_url"],
        "captured_at": concept.get("captured_at"),
        "domain": concept.get("domain"),
        "changed": True,
        "category": category,
        "summary": classification.get("summary", ""),
        "affected_concepts": classification.get("affected_concepts", []),
        "confidence": classification.get("confidence", 0.0),
        "should_notify": category in ("substantive", "breaking"),
        "current_hash": current_hash,
        "current_text_preview": current_text[:500],
        "fetched_at": fetched.get("fetched_at"),
        "metadata": {
            "computer": fetch.get("metadata", {}),
            "classifier": {"_stub": classification.get("_stub", False)},
        },
    }


def run_scan(user_id: str = None, max_concepts: int = 5) -> dict:
    """
    Top-level scan. Returns a structured response with all verdicts.
    """
    concepts = get_tracked_concepts(user_id=user_id, limit=max_concepts)
    verdicts = [scan_concept(c) for c in concepts]
    notifications = [v for v in verdicts if v.get("should_notify")]

    return {
        "user_id": user_id,
        "scanned_at": datetime.utcnow().isoformat(),
        "concepts_scanned": len(verdicts),
        "notifications_count": len(notifications),
        "verdicts": verdicts,
        "notifications": notifications,
        "modes": {
            "computer": "live" if perplexity_client.is_live() else "stub",
            "classifier": "live" if claude_client.is_live() else "stub",
        },
    }
