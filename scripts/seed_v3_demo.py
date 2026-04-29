#!/usr/bin/env python3
"""
Aasan V3 Demo Seed — Sarah Chen, Senior SWE at TechCorp

Populates a fresh backend with the V3 demo persona end-to-end:
  - Neo4j: 28 concept nodes spanning Cloud / Kubernetes / AWS / Networking
           / Security / Leadership (mix of mastered, weak, gap)
  - Mem0:  10 representative memories from Sarah's last 12 weeks
  - Drive: 5 training docs (uses /drive/index — stub returns the canned 5)
  - Path Engine: demo seed activates automatically for `demo-user`
                 (cloud-architect / compliance / mlops paths)

Idempotent: re-running upserts cleanly. Uses the live backend HTTP API,
so it works against local Flask, Render, or any deployment.

Usage:
    python scripts/seed_v3_demo.py
    python scripts/seed_v3_demo.py --user-id sarah@company.com
    python scripts/seed_v3_demo.py --backend https://aasan-backend.onrender.com

Env vars (or pass via flags):
    AASAN_BACKEND_URL  default: http://localhost:5000
    AASAN_SECRET       default: aasan-secret-2026
    AASAN_USER_ID      default: demo-user
"""

import os
import sys
import argparse
import requests

DEFAULT_BACKEND = os.environ.get("AASAN_BACKEND_URL", "http://localhost:5000")
DEFAULT_SECRET = os.environ.get("AASAN_SECRET", "aasan-secret-2026")
DEFAULT_USER = os.environ.get("AASAN_USER_ID", "demo-user")


# ──────────────────────────────────────────────────────────────
# 28 concepts — Sarah's knowledge graph
# (name, subject, domain, mastery_score, is_gap, gap_type, definition)
# ──────────────────────────────────────────────────────────────

CONCEPTS = [
    # Cloud & Kubernetes — strong foundation
    ("Linux Fundamentals",        "Cloud", "OS",          0.92, False, "none",          "Process model, file permissions, networking primitives, package management."),
    ("Container Basics",          "Cloud", "Containers",  0.88, False, "none",          "Image layers, registries, container runtimes, lifecycle."),
    ("Docker",                    "Cloud", "Containers",  0.85, False, "none",          "Dockerfile patterns, multi-stage builds, image hygiene, compose."),
    ("Kubernetes Architecture",   "Cloud", "Kubernetes",  0.82, False, "none",          "Control plane, nodes, etcd, kubelet, scheduler interactions."),
    ("Pods & Deployments",        "Cloud", "Kubernetes",  0.78, False, "none",          "Pod lifecycle, rolling updates, resource limits, probes."),
    ("Services & Networking",     "Cloud", "Kubernetes",  0.72, False, "none",          "ClusterIP, NodePort, LoadBalancer, Ingress, kube-proxy modes."),
    ("Service Mesh — Istio",      "Cloud", "Kubernetes",  0.55, True,  "shallow",       "Sidecars, VirtualService, DestinationRule, mTLS, traffic management."),
    ("mTLS Basics",               "Cloud", "Security",    0.40, True,  "critical_path", "Mutual TLS handshake, cert rotation, identity propagation."),
    ("Topology Spread Constraints","Cloud","Kubernetes",  0.30, True,  "critical_path", "K8s 1.31 deprecated topologyKeys; topologySpreadConstraints is the replacement."),

    # AWS — partial coverage with gaps
    ("AWS Core — EC2/S3/VPC",     "Cloud", "AWS",         0.65, False, "none",          "Compute, storage, networking primitives across regions and AZs."),
    ("IAM Roles & Policies",      "Cloud", "AWS",         0.45, True,  "critical_path", "Roles vs users, least-privilege policies, role assumption, MFA."),
    ("Cross-account Assume-Role", "Cloud", "AWS",         0.30, True,  "critical_path", "STS, trust policy, external IDs, session tagging."),
    ("Lambda & Serverless",       "Cloud", "AWS",         0.50, False, "none",          "Cold starts, concurrency, layers, event sources."),
    ("CloudFormation/CDK",        "Cloud", "AWS",         0.35, True,  "shallow",       "Stacks, drift, change sets; CDK constructs and synthesis."),
    ("Terraform Modules",         "Cloud", "IaC",         0.55, False, "none",          "Module composition, remote state with DynamoDB locking, workspaces."),

    # Security — weak
    ("Cloud Security Foundations","Cloud", "Security",    0.35, True,  "critical_path", "Shared responsibility model, encryption, KMS, secret rotation."),
    ("Secrets Management",        "Cloud", "Security",    0.45, True,  "shallow",       "Vault, AWS Secrets Manager, rotation patterns, scoping."),

    # Networking — known weak point
    ("TCP/UDP Fundamentals",      "Cloud", "Networking",  0.70, False, "none",          "Three-way handshake, ports, sockets, common tooling."),
    ("DNS & Load Balancing",      "Cloud", "Networking",  0.50, False, "none",          "Records, propagation, ALB vs NLB, target groups."),
    ("HTTP/2 & gRPC",             "Cloud", "Networking",  0.35, True,  "shallow",       "Multiplexing, server push, streaming, Protobuf."),

    # Observability
    ("Distributed Tracing",       "Cloud", "Observability", 0.55, False, "none",        "Spans, context propagation, OpenTelemetry, sampling."),
    ("Metrics & Dashboards",      "Cloud", "Observability", 0.65, False, "none",        "RED, USE methodologies; Prometheus, Grafana, alerting."),
    ("Structured Logging",        "Cloud", "Observability", 0.70, False, "none",        "JSON logs, correlation IDs, retention."),

    # MLOps — exploration goal
    ("MLOps — overview",          "ML",    "MLOps",       0.45, False, "none",          "Lifecycle of ML systems: data → train → deploy → monitor."),
    ("Model Serving",             "ML",    "MLOps",       0.30, False, "none",          "TF Serving, TorchServe, KServe, batch vs online inference."),
    ("Feature Stores",            "ML",    "MLOps",       0.20, True,  "shallow",       "Online/offline parity, feature versioning, point-in-time correctness."),

    # Leadership — beginner growth area
    ("1-on-1 Cadence",            "Leadership", "People", 0.40, True,  "shallow",       "Weekly 30-min cadence, agenda templates, skip-levels."),
    ("Feedback Frameworks (SBI)", "Leadership", "People", 0.25, True,  "shallow",       "Situation-Behavior-Impact; COIN; radical candor patterns."),
]

MEMORIES = [
    ("manager_assigned",     "Manager Raj assigned 'Data Privacy Compliance 2026' — due June 30, 2026."),
    ("learning_rhythm",      "Learns best in mornings — 9:00–11:00 AM is the productive window. Avoids end-of-day learning sessions."),
    ("primary_goal",         "Primary career goal: Become a Cloud Architect (Q4 2026) — promoted to Staff Engineer + leads our team's cloud migration."),
    ("weak_areas",           "Knows networking and IAM are weakest; flagged 'Cross-account Assume-Role' as blocking the migration plan."),
    ("recent_session",       "Last session covered Service Mesh with Istio (47 min, mastery 0.55) — flagged mTLS gap, want a Quickstart next."),
    ("currency_event",       "K8s 1.31 deprecation of topologyKeys triggered a 3-min refresher on topologySpreadConstraints (April 27)."),
    ("pivot_curiosity",      "Curious about MLOps — might bridge to next role; could combine with cloud architecture for an ML platform path."),
    ("management_interest",  "Considering pivot toward eng management in 2-3 years; started reading the 1-1 playbook from People Ops."),
    ("ai_resilience_concern","Worried about AI taking over routine ops work; wants up-the-stack moves and AI-resilient pivots tracked."),
    ("preferred_sources",    "Prefers internal Confluence + Coursera + LinkedIn Learning; rarely uses YouTube; AWS docs for reference only."),
]


def post(backend, secret, path, body):
    return requests.post(
        backend.rstrip("/") + path,
        headers={"X-Aasan-Secret": secret, "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )


def seed_concepts(backend, secret, user_id):
    print(f"\n[1/4] Seeding {len(CONCEPTS)} concepts to Neo4j...")
    ok = 0
    for name, subject, domain, mastery, is_gap, gap_type, definition in CONCEPTS:
        r = post(backend, secret, "/neo4j/write_concept", {
            "user_id": user_id,
            "name": name,
            "subject": subject,
            "domain": domain,
            "mastery_score": mastery,
            "confidence": mastery,
            "is_gap": is_gap,
            "gap_type": gap_type,
            "definition": definition,
        })
        if r.status_code == 200:
            ok += 1
        else:
            print(f"   ! {name}: {r.status_code} {r.text[:120]}")
    print(f"   ✓ {ok}/{len(CONCEPTS)} concepts written")
    return ok


def seed_memories(backend, secret, user_id):
    print(f"\n[2/4] Seeding {len(MEMORIES)} memories to Mem0...")
    ok = 0
    for tag, text in MEMORIES:
        r = post(backend, secret, "/mem0/add", {
            "user_id": user_id,
            "messages": [{"role": "user", "content": text}],
            "metadata": {"tag": tag, "seeded_by": "seed_v3_demo"},
        })
        if r.status_code == 200:
            ok += 1
        else:
            print(f"   ! {tag}: {r.status_code} {r.text[:120]}")
    print(f"   ✓ {ok}/{len(MEMORIES)} memories added")
    return ok


def seed_drive(backend, secret, user_id):
    print("\n[3/4] Running /drive/index — stubs 5 demo Drive docs into content_index + vector_index...")
    r = post(backend, secret, "/drive/index", {"limit": 25, "target_user_id": user_id})
    if r.status_code == 200:
        body = r.json()
        print(f"   ✓ ingested {body['counts']['ingested']}, vector_index total {body['counts']['vector_index_total']}")
        print(f"   modes: {body['modes']}")
        return body['counts']['ingested']
    print(f"   ! {r.status_code} {r.text[:200]}")
    return 0


def warm_path_engine(backend, secret, user_id):
    """Touches /goal/list to trigger the path_engine demo seed for demo-user."""
    print("\n[4/4] Warming Path Engine (auto-seeds 3 goals + paths for demo-user)...")
    r = post(backend, secret, "/goal/list", {"user_id": user_id})
    if r.status_code == 200:
        n = r.json().get("goal_count", 0)
        print(f"   ✓ {n} goals loaded for {user_id}")
        if user_id != "demo-user" and n == 0:
            print("   note: non-demo user starts empty — create goals via /goal/create or chat onboarding.")
        return n
    print(f"   ! {r.status_code} {r.text[:200]}")
    return 0


def health_check(backend, secret):
    print(f"\nHealth check: {backend}/health ...")
    try:
        r = requests.get(backend.rstrip("/") + "/health", timeout=10)
        print(f"   ✓ {r.status_code} {r.text[:100]}")
        return r.status_code == 200
    except Exception as exc:
        print(f"   ✗ {exc}")
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--backend", default=DEFAULT_BACKEND, help=f"Backend URL (default: {DEFAULT_BACKEND})")
    parser.add_argument("--secret",  default=DEFAULT_SECRET,  help="X-Aasan-Secret header")
    parser.add_argument("--user-id", default=DEFAULT_USER,    help=f"User to seed (default: {DEFAULT_USER})")
    parser.add_argument("--skip-health", action="store_true", help="Skip /health pre-flight")
    args = parser.parse_args()

    print("=" * 70)
    print(f"Aasan V3 Demo Seed → {args.user_id} on {args.backend}")
    print("=" * 70)

    if not args.skip_health and not health_check(args.backend, args.secret):
        print("\n⚠ Backend not reachable. Set AASAN_BACKEND_URL or pass --backend.")
        sys.exit(1)

    concepts = seed_concepts(args.backend, args.secret, args.user_id)
    memories = seed_memories(args.backend, args.secret, args.user_id)
    drive = seed_drive(args.backend, args.secret, args.user_id)
    goals = warm_path_engine(args.backend, args.secret, args.user_id)

    print("\n" + "=" * 70)
    print(f"Done. concepts={concepts}, memories={memories}, drive_docs={drive}, goals={goals}")
    print("=" * 70)
    print(f"\nNext: open the app, sign in as {args.user_id}, run the demo script.")


if __name__ == "__main__":
    main()
