"""
Aasan V3 — Peraasan Backend
Deployed on Render.com

Endpoints:
1. /neo4j/*       — Knowledge graph (Neo4j AuraDB)
2. /mem0/*        — Persistent memory (Mem0)
3. /content/*     — Content index CRUD (in-memory for Phase 1, Airtable later)
4. /review/*      — Spaced review scheduling
5. /capture/*     — Knowledge capture (save learning session results)
6. /clerk/*       — Clerk webhook receiver
7. /health        — Health check
8. /agent/*       — V3 deep agentic layer (Perplexity Computer wrapper)
9. /freshness/*   — V3 Currency Watch (uses /agent + Claude classifier)

Called by: React app (browser), the React app calls Render endpoints which
in turn call Perplexity Computer (server-side) and Claude (server-side).
The Peraasan Agent Bridge Chrome extension is browser-side only — it does
not talk to this backend directly.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from neo4j import GraphDatabase
from mem0 import MemoryClient
import os
import json
from datetime import datetime, timedelta

# V3: deep-agentic + reasoning service modules
from services import perplexity_client, claude_client, freshness, career, predigest, path_engine, sme, stay_ahead, career_simulator

app = Flask(__name__)
CORS(app)  # Allow all origins — needed for browser graph visualisation

# ─────────────────────────────────────────────
# Configuration — set these as Render env vars
# ─────────────────────────────────────────────
NEO4J_URI = os.environ.get("NEO4J_URI")
NEO4J_USERNAME = os.environ.get("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
MEM0_API_KEY = os.environ.get("MEM0_API_KEY")
API_SECRET = os.environ.get("API_SECRET", "aasan-secret-2026")

# ─────────────────────────────────────────────
# Clients — initialised once at startup
# ─────────────────────────────────────────────
neo4j_driver = None
mem0_client = None

def get_neo4j_driver():
    global neo4j_driver
    if neo4j_driver is None and NEO4J_URI and NEO4J_PASSWORD:
        neo4j_driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
        )
    return neo4j_driver

def get_mem0_client():
    global mem0_client
    if mem0_client is None and MEM0_API_KEY:
        mem0_client = MemoryClient(api_key=MEM0_API_KEY)
    return mem0_client

def verify_secret(req):
    """Simple API secret verification."""
    secret = req.headers.get("X-Aasan-Secret") or req.json.get("secret", "")
    return secret == API_SECRET

# ─────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "aasan-peraasan-backend"})

# ─────────────────────────────────────────────
# NEO4J ENDPOINTS
# ─────────────────────────────────────────────

def serialize_neo4j(value):
    """Convert Neo4j types to JSON-serializable Python types."""
    from neo4j.time import DateTime, Date
    if isinstance(value, (DateTime, Date)):
        return str(value)
    return value

def serialize_node(node):
    """Convert a Neo4j node to a JSON-serializable dict."""
    return {k: serialize_neo4j(v) for k, v in dict(node).items()}


@app.route("/neo4j/write_concept", methods=["POST"])
def write_concept():
    """
    Write or update a concept node in Neo4j.
    Called by Make.com after each concept extraction.

    Expected payload:
    {
        "user_id": "demo-arjun",
        "name": "Photosynthesis",
        "definition": "The process by which plants...",
        "subject": "Biology",
        "domain": "Plant Physiology",
        "confidence": 0.95,
        "is_new": true,
        "is_gap": false,
        "gap_type": "none",
        "connects_to": ["Chloroplasts", "Chlorophyll"],
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    driver = get_neo4j_driver()

    if not driver:
        return jsonify({"error": "Neo4j not configured"}), 500

    try:
        with driver.session() as session:
            # Write or update the concept node
            session.run("""
                MERGE (c:Concept {name: $name, user_id: $user_id})
                ON CREATE SET
                    c.id = randomUUID(),
                    c.definition = $definition,
                    c.subject = $subject,
                    c.domain = $domain,
                    c.mastery_score = $confidence,
                    c.confidence = $confidence,
                    c.is_gap = $is_gap,
                    c.gap_type = $gap_type,
                    c.first_captured = datetime(),
                    c.last_reinforced = datetime(),
                    c.capture_count = 1
                ON MATCH SET
                    c.mastery_score = (c.mastery_score + $confidence) / 2,
                    c.confidence = (c.confidence + $confidence) / 2,
                    c.is_gap = $is_gap,
                    c.gap_type = $gap_type,
                    c.last_reinforced = datetime(),
                    c.capture_count = c.capture_count + 1
            """,
                name=data.get("name"),
                user_id=data.get("user_id"),
                definition=data.get("definition", ""),
                subject=data.get("subject", ""),
                domain=data.get("domain", ""),
                confidence=float(data.get("confidence", 0.5)),
                is_gap=data.get("is_gap", False),
                gap_type=data.get("gap_type", "none")
            )

            # Create relationships to connected concepts
            connects_to = data.get("connects_to", [])
            for related_name in connects_to:
                session.run("""
                    MATCH (a:Concept {name: $name, user_id: $user_id})
                    MERGE (b:Concept {name: $related_name, user_id: $user_id})
                    ON CREATE SET b.id = randomUUID(), b.capture_count = 0
                    MERGE (a)-[:CONNECTS_TO]->(b)
                """,
                    name=data.get("name"),
                    user_id=data.get("user_id"),
                    related_name=related_name
                )

        return jsonify({
            "status": "ok",
            "concept": data.get("name"),
            "connections_created": len(data.get("connects_to", []) if isinstance(data.get("connects_to"), list) else [])
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/neo4j/get_concepts", methods=["POST"])
def get_concepts():
    """
    Get all concepts for a learner — used by Neovis.js graph.

    Expected payload:
    {
        "user_id": "demo-arjun",
        "limit": 200,
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    driver = get_neo4j_driver()

    if not driver:
        return jsonify({"error": "Neo4j not configured"}), 500

    try:
        with driver.session() as session:
            result = session.run("""
                MATCH (c:Concept {user_id: $user_id})
                OPTIONAL MATCH (c)-[r:CONNECTS_TO]->(d:Concept {user_id: $user_id})
                RETURN c, r, d
                LIMIT $limit
            """,
                user_id=data.get("user_id"),
                limit=data.get("limit", 200)
            )

            concepts = []
            for record in result:
                concept = serialize_node(record["c"])
                concepts.append(concept)

        return jsonify({"status": "ok", "concepts": concepts, "count": len(concepts)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/neo4j/get_graph_data", methods=["POST"])
def get_graph_data():
    """
    Get nodes and relationships for Neovis.js visualisation.

    Expected payload:
    {
        "user_id": "demo-arjun",
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    driver = get_neo4j_driver()

    if not driver:
        return jsonify({"error": "Neo4j not configured"}), 500

    try:
        with driver.session() as session:
            result = session.run("""
                MATCH (c:Concept {user_id: $user_id})
                OPTIONAL MATCH (c)-[r:CONNECTS_TO]->(d:Concept {user_id: $user_id})
                RETURN 
                    collect(distinct {
                        id: c.id,
                        name: c.name,
                        subject: c.subject,
                        mastery_score: c.mastery_score,
                        is_gap: c.is_gap,
                        gap_type: c.gap_type
                    }) as nodes,
                    collect(distinct {
                        from: c.name,
                        to: d.name
                    }) as relationships
            """,
                user_id=data.get("user_id")
            )

            record = result.single()
            return jsonify({
                "status": "ok",
                "nodes": record["nodes"] if record else [],
                "relationships": [r for r in record["relationships"] if r["to"]] if record else []
            })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
# MEM0 ENDPOINTS
# ─────────────────────────────────────────────

@app.route("/mem0/add", methods=["POST"])
def mem0_add():
    """
    Add a memory for a learner.
    Called by Make.com after each concept extraction or session end.

    Expected payload:
    {
        "user_id": "demo-arjun",
        "content": "Learner has strong understanding of Photosynthesis in Biology",
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    client = get_mem0_client()

    if not client:
        return jsonify({"error": "Mem0 not configured"}), 500

    try:
        result = client.add(
            messages=[{"role": "user", "content": data.get("content")}],
            user_id=data.get("user_id")
        )
        return jsonify({"status": "ok", "result": result})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/mem0/search", methods=["POST"])
def mem0_search():
    """
    Search memories for a learner — called at session start.
    Returns most relevant memories for the current topic.

    Expected payload:
    {
        "user_id": "demo-arjun",
        "query": "what does this learner know about mathematics",
        "limit": 20,
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    client = get_mem0_client()

    if not client:
        return jsonify({"error": "Mem0 not configured"}), 500

    try:
        results = client.search(
            query=data.get("query"),
            user_id=data.get("user_id"),
            limit=data.get("limit", 20)
        )

        # Format memories as a clean text block for Claude context
        memories_text = "\n".join([
            f"- {r.get('memory', '')}"
            for r in results
            if r.get('memory')
        ])

        return jsonify({
            "status": "ok",
            "memories": results,
            "memories_text": memories_text,
            "count": len(results)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/mem0/add_session_summary", methods=["POST"])
def mem0_add_session_summary():
    """
    Add a complete session summary as structured memories.
    Called at session end with the summariser output.

    Expected payload:
    {
        "user_id": "demo-arjun",
        "session_headline": "Arjun explored photosynthesis...",
        "concepts": [...],
        "gaps": [...],
        "subject_classification": "Biology",
        "readiness_signal": "...",
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    client = get_mem0_client()
    user_id = data.get("user_id")

    if not client:
        return jsonify({"error": "Mem0 not configured"}), 500

    try:
        memories_added = 0

        # Add session headline
        client.add(
            messages=[{"role": "user", "content": f"Session completed: {data.get('session_headline')}"}],
            user_id=user_id
        )
        memories_added += 1

        # Add each concept as a memory
        for concept in data.get("concepts", []):
            name = concept.get("name", "")
            status = concept.get("status", "new")
            gap_type = concept.get("gap_type", "none")

            if status == "gap":
                memory = f"Learner has a {gap_type} gap in: {name} ({data.get('subject_classification', '')})"
            elif status == "reinforced":
                memory = f"Learner reinforced understanding of: {name} ({data.get('subject_classification', '')})"
            else:
                memory = f"Learner learned: {name} in {data.get('subject_classification', '')} — {status}"

            client.add(
                messages=[{"role": "user", "content": memory}],
                user_id=user_id
            )
            memories_added += 1

        # Add readiness signal
        if data.get("readiness_signal"):
            client.add(
                messages=[{"role": "user", "content": f"Readiness signal: {data.get('readiness_signal')}"}],
                user_id=user_id
            )
            memories_added += 1

        return jsonify({
            "status": "ok",
            "memories_added": memories_added
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
# CONTENT INDEX ENDPOINTS
# In-memory store for Phase 1. Migrate to Airtable/DB later.
# Perplexity Computer calls these to index content it discovers.
# ─────────────────────────────────────────────

content_index = []  # In-memory for Phase 1


@app.route("/content/add", methods=["POST"])
def content_add():
    """
    Add a content item to the index.
    Called by: Perplexity Computer (after crawling/classifying)
    or React app (manager upload).

    Expected payload:
    {
        "title": "Kubernetes Architecture Overview",
        "source": "coursera",
        "source_url": "https://...",
        "content_type": "video",
        "duration_minutes": 45,
        "difficulty": "intermediate",
        "skills": ["kubernetes", "containers"],
        "concepts_covered": ["pods", "services"],
        "prerequisites": ["container_basics"],
        "ai_summary": "Covers the core building blocks...",
        "quality_score": 0.87,
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    data["content_id"] = f"content-{len(content_index) + 1}"
    data["indexed_at"] = datetime.utcnow().isoformat()
    content_index.append(data)

    return jsonify({
        "status": "ok",
        "content_id": data["content_id"],
        "total_indexed": len(content_index)
    })


@app.route("/content/search", methods=["POST"])
def content_search():
    """
    Search the content index by query.
    Called by: React app (when Peraasan needs content for recommendations).

    Expected payload:
    {
        "query": "kubernetes networking",
        "limit": 10,
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    query = data.get("query", "").lower()
    limit = data.get("limit", 10)

    # Simple keyword search for Phase 1
    results = []
    for item in content_index:
        searchable = json.dumps(item).lower()
        if query in searchable or any(q in searchable for q in query.split()):
            results.append(item)
        if len(results) >= limit:
            break

    return jsonify({
        "status": "ok",
        "results": results,
        "count": len(results)
    })


@app.route("/content/list", methods=["POST"])
def content_list():
    """
    List all indexed content.
    Called by: React app (for sources panel).
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    return jsonify({
        "status": "ok",
        "content": content_index,
        "count": len(content_index)
    })


# ─────────────────────────────────────────────
# KNOWLEDGE CAPTURE ENDPOINTS
# Called after a learning session to persist what was learned.
# Browser calls Claude to extract → then calls these to save.
# ─────────────────────────────────────────────

@app.route("/capture/session", methods=["POST"])
def capture_session():
    """
    Save a complete learning session — concepts to Neo4j + memory to Mem0.
    Called by: React app after Claude extracts concepts from a session.

    Expected payload:
    {
        "user_id": "emp-sarah-001",
        "session_title": "Kubernetes Networking",
        "concepts": [
            {"name": "ClusterIP", "definition": "...", "subject": "Cloud",
             "domain": "Kubernetes", "confidence": 0.7, "is_gap": false,
             "gap_type": "none", "connects_to": ["Services"]}
        ],
        "gaps": ["Service Mesh"],
        "summary": "Learned about Kubernetes networking...",
        "duration_minutes": 25,
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    user_id = data.get("user_id")
    driver = get_neo4j_driver()
    client = get_mem0_client()
    concepts_saved = 0
    memories_added = 0

    # Save each concept to Neo4j
    if driver:
        try:
            with driver.session() as session:
                for concept in data.get("concepts", []):
                    session.run("""
                        MERGE (c:Concept {name: $name, user_id: $user_id})
                        ON CREATE SET
                            c.id = randomUUID(),
                            c.definition = $definition,
                            c.subject = $subject,
                            c.domain = $domain,
                            c.mastery_score = $confidence,
                            c.confidence = $confidence,
                            c.is_gap = $is_gap,
                            c.gap_type = $gap_type,
                            c.first_captured = datetime(),
                            c.last_reinforced = datetime(),
                            c.capture_count = 1
                        ON MATCH SET
                            c.mastery_score = (c.mastery_score + $confidence) / 2,
                            c.confidence = (c.confidence + $confidence) / 2,
                            c.is_gap = $is_gap,
                            c.gap_type = $gap_type,
                            c.last_reinforced = datetime(),
                            c.capture_count = c.capture_count + 1
                    """,
                        name=concept.get("name"),
                        user_id=user_id,
                        definition=concept.get("definition", ""),
                        subject=concept.get("subject", ""),
                        domain=concept.get("domain", ""),
                        confidence=float(concept.get("confidence", 0.5)),
                        is_gap=concept.get("is_gap", False),
                        gap_type=concept.get("gap_type", "none")
                    )
                    # Create relationships
                    for related in concept.get("connects_to", []):
                        session.run("""
                            MATCH (a:Concept {name: $name, user_id: $user_id})
                            MERGE (b:Concept {name: $related, user_id: $user_id})
                            ON CREATE SET b.id = randomUUID(), b.capture_count = 0
                            MERGE (a)-[:CONNECTS_TO]->(b)
                        """, name=concept.get("name"), user_id=user_id, related=related)
                    concepts_saved += 1
        except Exception as e:
            return jsonify({"error": f"Neo4j error: {str(e)}"}), 500

    # Save session summary to Mem0
    if client:
        try:
            summary = data.get("summary", "")
            if summary:
                client.add(
                    messages=[{"role": "user", "content": f"Session completed: {summary}"}],
                    user_id=user_id
                )
                memories_added += 1

            # Save each gap as a memory
            for gap in data.get("gaps", []):
                client.add(
                    messages=[{"role": "user", "content": f"Gap detected: {gap} — needs attention"}],
                    user_id=user_id
                )
                memories_added += 1
        except Exception as e:
            return jsonify({"error": f"Mem0 error: {str(e)}"}), 500

    return jsonify({
        "status": "ok",
        "concepts_saved": concepts_saved,
        "memories_added": memories_added
    })


# ─────────────────────────────────────────────
# SPACED REVIEW ENDPOINTS
# In-memory review queue for Phase 1.
# Called by: React app on login to check due reviews.
# ─────────────────────────────────────────────

review_queue = {}  # In-memory: { user_id: [{ concept, next_review, interval, ease }] }


@app.route("/review/schedule", methods=["POST"])
def review_schedule():
    """
    Schedule a concept for spaced review.
    Called by: React app after knowledge capture.

    Expected payload:
    {
        "user_id": "emp-sarah-001",
        "concept_name": "Kubernetes Pods",
        "initial_mastery": 0.7,
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    user_id = data.get("user_id")

    if user_id not in review_queue:
        review_queue[user_id] = []

    # Don't duplicate
    existing = [r for r in review_queue[user_id] if r["concept"] == data.get("concept_name")]
    if not existing:
        review_queue[user_id].append({
            "concept": data.get("concept_name"),
            "next_review": (datetime.utcnow() + timedelta(days=1)).isoformat(),
            "interval_days": 1,
            "ease_factor": 2.5,
            "review_count": 0,
            "mastery": data.get("initial_mastery", 0.5)
        })

    return jsonify({"status": "ok", "reviews_queued": len(review_queue[user_id])})


@app.route("/review/due", methods=["POST"])
def review_due():
    """
    Get concepts due for review today.
    Called by: React app on login / session start.

    Expected payload:
    {
        "user_id": "emp-sarah-001",
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    user_id = data.get("user_id")
    now = datetime.utcnow().isoformat()

    due = [r for r in review_queue.get(user_id, []) if r["next_review"] <= now]

    return jsonify({
        "status": "ok",
        "due": due,
        "count": len(due)
    })


@app.route("/review/complete", methods=["POST"])
def review_complete():
    """
    Record a review result — update interval using SM-2 algorithm.
    Called by: React app after employee answers a review question.

    Expected payload:
    {
        "user_id": "emp-sarah-001",
        "concept_name": "Kubernetes Pods",
        "rating": 3,
        "secret": "aasan-secret-2026"
    }
    rating: 1=forgot, 2=hard, 3=good, 4=easy
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    user_id = data.get("user_id")
    concept_name = data.get("concept_name")
    rating = data.get("rating", 3)

    reviews = review_queue.get(user_id, [])
    for r in reviews:
        if r["concept"] == concept_name:
            r["review_count"] += 1
            if rating >= 3:
                # SM-2: increase interval
                r["ease_factor"] = max(1.3, r["ease_factor"] + 0.1 * (rating - 3))
                r["interval_days"] = int(r["interval_days"] * r["ease_factor"])
                r["mastery"] = min(1.0, r["mastery"] + 0.05 * rating)
            else:
                # Reset interval
                r["interval_days"] = 1
                r["ease_factor"] = max(1.3, r["ease_factor"] - 0.2)
                r["mastery"] = max(0.0, r["mastery"] - 0.1)
            r["next_review"] = (datetime.utcnow() + timedelta(days=r["interval_days"])).isoformat()
            break

    # Update mastery in Neo4j
    driver = get_neo4j_driver()
    if driver:
        try:
            with driver.session() as session:
                mastery_val = next((r["mastery"] for r in reviews if r["concept"] == concept_name), 0.5)
                session.run("""
                    MATCH (c:Concept {name: $name, user_id: $user_id})
                    SET c.mastery_score = $mastery, c.last_reinforced = datetime()
                """, name=concept_name, user_id=user_id, mastery=mastery_val)
        except Exception:
            pass

    return jsonify({"status": "ok", "next_review_days": r["interval_days"] if 'r' in dir() else 1})


# ─────────────────────────────────────────────
# CONTEXT ENDPOINT
# Single call to get everything React needs on login.
# Reduces round trips: one call → graph + memory + reviews.
# ─────────────────────────────────────────────

@app.route("/context/load", methods=["POST"])
def context_load():
    """
    Load full employee context on login — knowledge graph + memories + due reviews.
    Called by: React app on sign-in.

    Expected payload:
    {
        "user_id": "emp-sarah-001",
        "secret": "aasan-secret-2026"
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    user_id = data.get("user_id")
    result = {"status": "ok", "user_id": user_id}

    # Get knowledge graph summary
    driver = get_neo4j_driver()
    if driver:
        try:
            with driver.session() as session:
                # Concept count + gap count
                stats = session.run("""
                    MATCH (c:Concept {user_id: $user_id})
                    RETURN count(c) as total,
                           sum(CASE WHEN c.is_gap = true THEN 1 ELSE 0 END) as gaps,
                           avg(c.mastery_score) as avg_mastery
                """, user_id=user_id).single()
                result["knowledge"] = {
                    "total_concepts": stats["total"] if stats else 0,
                    "gaps": stats["gaps"] if stats else 0,
                    "avg_mastery": round(float(stats["avg_mastery"] or 0), 2) if stats else 0
                }
        except Exception as e:
            result["knowledge"] = {"error": str(e)}

    # Get recent memories
    client = get_mem0_client()
    if client:
        try:
            memories = client.search(
                query="recent learning progress and goals",
                user_id=user_id,
                limit=10
            )
            result["memories"] = [m.get("memory", "") for m in memories if m.get("memory")]
        except Exception as e:
            result["memories"] = {"error": str(e)}

    # Get due reviews
    now = datetime.utcnow().isoformat()
    due = [r for r in review_queue.get(user_id, []) if r["next_review"] <= now]
    result["reviews_due"] = due

    # Get content count
    result["content_indexed"] = len(content_index)

    return jsonify(result)


# ─────────────────────────────────────────────
# CLERK WEBHOOK
# Receives user.created events from Clerk.
# Creates employee record in Mem0.
# ─────────────────────────────────────────────

@app.route("/clerk/webhook", methods=["POST"])
def clerk_webhook():
    """
    Receive Clerk webhook events.
    Configure in Clerk Dashboard → Webhooks → endpoint: https://aasan-backend.onrender.com/clerk/webhook
    """
    data = request.json
    event_type = data.get("type", "")

    if event_type == "user.created":
        user_data = data.get("data", {})
        user_id = user_data.get("id", "")
        email = ""
        if user_data.get("email_addresses"):
            email = user_data["email_addresses"][0].get("email_address", "")
        first_name = user_data.get("first_name", "")

        # Create initial memory in Mem0
        client = get_mem0_client()
        if client and user_id:
            try:
                client.add(
                    messages=[{"role": "user", "content": f"New employee registered: {first_name} ({email}). No learning history yet. Goals not set."}],
                    user_id=user_id
                )
            except Exception:
                pass

        return jsonify({"status": "ok", "user_id": user_id})

    return jsonify({"status": "ok", "event": event_type})


# ─────────────────────────────────────────────
# V3 — Deep Agentic Layer (Perplexity Computer)
# Generic /agent/computer_run is the single passthrough; specific consumers
# (currency, career, content) call through here so we have one place that
# routes to the agentic backend. Stub mode is automatic when
# PERPLEXITY_API_KEY is unset.
# ─────────────────────────────────────────────

@app.route("/agent/status", methods=["GET"])
def agent_status():
    """Quick check: is Perplexity Computer connected? Is Anthropic key set?"""
    return jsonify({
        "perplexity_computer": {
            "live": perplexity_client.is_live(),
            "mode": "live" if perplexity_client.is_live() else "stub",
        },
        "claude": {
            "live": claude_client.is_live(),
            "mode": "live" if claude_client.is_live() else "stub",
        },
    })


@app.route("/agent/computer_run", methods=["POST"])
def agent_computer_run():
    """
    Generic Perplexity Computer pass-through.
    Body: { "task": { "kind": ..., "input": ..., "constraints": ... }, "secret": ... }
    Returns whatever perplexity_client.run_task returns.
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json or {}
    task = data.get("task")
    if not isinstance(task, dict) or "kind" not in task:
        return jsonify({"error": "Invalid task — must be a dict with 'kind' field"}), 400

    timeout_s = int(data.get("timeout_s", 60))
    result = perplexity_client.run_task(task, timeout_s=timeout_s)
    return jsonify(result)


# ─────────────────────────────────────────────
# V3 — Currency Watch (External Freshness)
# Re-fetch a source via Perplexity Computer, classify the change with Claude,
# return a verdict. Notification persistence is layered above this endpoint
# (caller decides whether to write to a Notifications table + chat queue).
# ─────────────────────────────────────────────

@app.route("/freshness/check", methods=["POST"])
def freshness_check():
    """
    Check whether a tracked source has materially changed since baseline.

    Body: {
      "source_url": "https://kubernetes.io/blog/...",
      "baseline_text": "<previously cached main_text>",
      "baseline_hash": "<sha256 of baseline_text>" (optional — recomputed if missing),
      "context": { "concept_name": "...", "captured_at": "...", ... } (optional),
      "secret": "..."
    }

    Returns: {
      "changed": bool,
      "category": "cosmetic|clarification|substantive|breaking",
      "summary": "...",
      "should_notify": bool,
      "current_text": "<truncated>",
      "current_hash": "...",
      "fetched_at": "...",
      "metadata": { computer + classifier metadata }
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json or {}
    source_url = data.get("source_url", "").strip()
    baseline_text = data.get("baseline_text", "")
    baseline_hash = data.get("baseline_hash") or ""
    context = data.get("context", {}) or {}

    if not source_url:
        return jsonify({"error": "source_url is required"}), 400

    # 1. Re-fetch via Perplexity Computer
    fetch_result = perplexity_client.fetch_url(source_url)
    if fetch_result.get("status") != "ok":
        return jsonify({
            "changed": False,
            "category": "error",
            "summary": "Could not re-fetch source.",
            "should_notify": False,
            "fetch_metadata": fetch_result,
        }), 200  # Soft fail — caller can retry

    fetched = fetch_result.get("result", {})
    current_text = fetched.get("main_text", "")
    current_hash = fetched.get("content_hash", "")

    # 2. Cheap diff — if hashes match, skip the classifier entirely
    if baseline_hash and baseline_hash == current_hash:
        return jsonify({
            "changed": False,
            "category": "cosmetic",
            "summary": "No change detected (content hash matches baseline).",
            "should_notify": False,
            "current_hash": current_hash,
            "fetched_at": fetched.get("fetched_at"),
            "metadata": {
                "computer": fetch_result.get("metadata", {}),
                "classifier": {"skipped": "hash_match"},
            },
        })

    # 3. Substance classifier (Claude)
    classification = claude_client.classify_change(
        old_text=baseline_text,
        new_text=current_text,
        context=context,
    )

    category = classification.get("category", "cosmetic")
    should_notify = category in ("substantive", "breaking")

    return jsonify({
        "changed": True,
        "category": category,
        "summary": classification.get("summary", ""),
        "affected_concepts": classification.get("affected_concepts", []),
        "confidence": classification.get("confidence", 0.0),
        "should_notify": should_notify,
        "current_text": current_text[:2000],  # truncate for response payload size
        "current_hash": current_hash,
        "fetched_at": fetched.get("fetched_at"),
        "metadata": {
            "computer": fetch_result.get("metadata", {}),
            "classifier": {"_stub": classification.get("_stub", False)},
        },
    })


@app.route("/freshness/scan", methods=["POST"])
def freshness_scan():
    """
    Run a Currency Watch scan over a user's tracked concepts.

    Body: { "user_id": str, "max_concepts": int (default 5), "secret": ... }

    For each tracked concept, runs the freshness pipeline:
      Perplexity Computer fetch_url → diff → Claude substance classify
      → categorize cosmetic / clarification / substantive / breaking
      → notify only on substantive + breaking

    Returns: {
      user_id, scanned_at, concepts_scanned, notifications_count,
      verdicts: [...all scanned, with category + summary],
      notifications: [...subset that warrant a chat surfacing],
      modes: { computer: live|stub, classifier: live|stub }
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json or {}
    user_id = data.get("user_id")
    max_concepts = int(data.get("max_concepts", 5))

    result = freshness.run_scan(user_id=user_id, max_concepts=max_concepts)
    return jsonify(result)


# ─────────────────────────────────────────────
# V3 — Career Compass / Market Watch
# Three watcher pipelines (role market, course launches, vendor certs)
# all running through Perplexity Computer scrape_pattern.
# ─────────────────────────────────────────────

@app.route("/career/scan", methods=["POST"])
def career_scan():
    """
    Run a Career Compass scan — produces Career_Signals across role market,
    course launches, vendor certs.

    Body: {
      "user_id": str,
      "target_role": str (optional, defaults to demo role),
      "max_signals": int (default 10),
      "secret": ...
    }

    Returns: {
      user_id, target_role, scanned_at, signals_count,
      signals_by_type: { role_skill_shift, new_course, vendor_cert },
      signals: [
        { signal_type, title, body, relevance_score, content_ref, detected_at }, ...
      ],
      modes: { computer: live|stub, classifier: live|stub }
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json or {}
    user_id = data.get("user_id")
    target_role = data.get("target_role")
    max_signals = int(data.get("max_signals", 10))

    result = career.run_scan(user_id=user_id, target_role=target_role, max_signals=max_signals)
    return jsonify(result)


# ─────────────────────────────────────────────
# V3 — Content Pre-digestion (third Perplexity Computer use case)
# Deep-read a single long doc URL, return structured 5-concept digest.
# ─────────────────────────────────────────────

@app.route("/agent/predigest", methods=["POST"])
def agent_predigest():
    """
    Pre-digest a long URL — Perplexity Computer fetches deeply, Claude extracts
    structured concepts + TL;DR + suggested next step.

    Body: {
      "url": "https://...",
      "learner_context": { "goal": "...", "current_path_step": "..." } (optional),
      "secret": ...
    }

    Returns: {
      url, title, source_domain,
      tldr, key_concepts, reading_time_saved_minutes,
      suggested_next_step,
      modes: { computer, classifier },
      fetched_at,
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json or {}
    url = (data.get("url") or "").strip()
    learner_context = data.get("learner_context") or {}

    if not url:
        return jsonify({"error": "url is required"}), 400

    result = predigest.predigest(url=url, learner_context=learner_context)
    return jsonify(result)


# ─────────────────────────────────────────────
# V3 — Path Engine (Live Persistent Learning Paths)
# Each goal owns one path. Engine adjusts on triggers; manual learner
# edits are sacred. Phase 1 store: in-memory dict per user_id.
# ─────────────────────────────────────────────

@app.route("/goal/list", methods=["POST"])
def goal_list():
    """List all active goals + path summary per goal for the user."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    return jsonify(path_engine.list_goals(user_id))


@app.route("/path/get", methods=["POST"])
def path_get():
    """Fetch the full ordered path for a single goal."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_id = data.get("goal_id")
    if not goal_id:
        return jsonify({"error": "goal_id required"}), 400
    return jsonify(path_engine.get_path(user_id, goal_id))


@app.route("/path/recompute", methods=["POST"])
def path_recompute():
    """
    Run the Path Adjustment Engine. Returns the diff that was applied.

    Body: {
      "user_id": str,
      "goal_id": str,
      "trigger": "session_complete" | "content_added" | "staleness_flag" | "assignment_create" | "learner_edit",
      "trigger_payload": {} (optional, trigger-specific)
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_id = data.get("goal_id")
    trigger = data.get("trigger", "session_complete")
    payload = data.get("trigger_payload", {}) or {}
    if not goal_id:
        return jsonify({"error": "goal_id required"}), 400
    return jsonify(path_engine.recompute(user_id, goal_id, trigger, payload))


@app.route("/path/insert_step", methods=["POST"])
def path_insert_step():
    """
    Manual learner edit — insert a step. Marked inserted_by=learner — sacred to engine.

    Body: { user_id, goal_id, step: { title, order?, step_type?, estimated_minutes?, inserted_reason? } }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_id = data.get("goal_id")
    step = data.get("step", {}) or {}
    if not goal_id or not step.get("title"):
        return jsonify({"error": "goal_id and step.title required"}), 400
    return jsonify(path_engine.insert_step_manual(user_id, goal_id, step))


# ─────────────────────────────────────────────
# V3 — SME Marketplace V1
# Internal directory (auto-derived in Phase 2; demo seed in Phase 1) +
# external curated marketplace + booking flow.
# ─────────────────────────────────────────────

@app.route("/sme/find", methods=["POST"])
def sme_find():
    """Match SMEs against a topic. Returns ranked list of internal + external."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    topic = data.get("topic", "").strip()
    learner_id = data.get("learner_id", "demo-user")
    limit = int(data.get("limit", 5))
    if not topic:
        return jsonify({"error": "topic required"}), 400
    return jsonify(sme.find_smes(topic=topic, learner_id=learner_id, limit=limit))


@app.route("/sme/book", methods=["POST"])
def sme_book():
    """Book a session with an SME (mock confirmation in Phase 1)."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    sme_id = data.get("sme_id")
    learner_id = data.get("learner_id", "demo-user")
    topic = data.get("topic", "")
    slot = data.get("slot")
    if not sme_id:
        return jsonify({"error": "sme_id required"}), 400
    return jsonify(sme.book_sme(sme_id=sme_id, learner_id=learner_id, topic=topic, slot=slot))


@app.route("/sme/bookings", methods=["POST"])
def sme_bookings():
    """List a learner's bookings."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    learner_id = data.get("learner_id", "demo-user")
    return jsonify(sme.list_bookings(learner_id=learner_id))


@app.route("/sme/register", methods=["POST"])
def sme_register():
    """Internal employee opts in as an SME."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    employee_id = data.get("employee_id")
    profile = data.get("profile", {}) or {}
    if not employee_id:
        return jsonify({"error": "employee_id required"}), 400
    return jsonify(sme.register_internal_sme(employee_id=employee_id, profile=profile))


# ─────────────────────────────────────────────
# V3 — Career Compass / Stay Ahead
# Mobility intelligence beyond training: best-fit roles, stretch roles,
# pivot options, hands-on experiences, market risk signal.
# ─────────────────────────────────────────────

@app.route("/career/stay_ahead", methods=["POST"])
def career_stay_ahead():
    """Run a Stay Ahead scan — 5-section career mobility digest."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    profile = data.get("profile")
    return jsonify(stay_ahead.run_stay_ahead(user_id=user_id, profile=profile))


@app.route("/career/simulate", methods=["POST"])
def career_simulate():
    """
    Career Scenario Planning Simulator — project 12-24 month outcomes for
    2-3 candidate paths with probabilistic ranges.
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    scenarios = data.get("scenarios")  # list of {id, name, description, effort_hours_per_week, horizon_months}
    profile = data.get("profile") or {}
    return jsonify(career_simulator.run_simulation(user_id=user_id, scenarios=scenarios, profile=profile))


# ─────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
