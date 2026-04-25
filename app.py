"""
Aasan — Peraasan Backend Scripts
Deployed on Render.com (free tier)

Two endpoints:
1. /neo4j/* — Knowledge graph writer (Neo4j AuraDB)
2. /mem0/* — Persistent memory bridge (Mem0)
3. /health — Health check

Make.com calls these endpoints from its HTTP modules.
"""

from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from mem0 import MemoryClient
import os

app = Flask(__name__)

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
            "connections_created": len(connects_to)
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
                concept = dict(record["c"])
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
            filters={"user_id": data.get("user_id")},
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
# Run
# ─────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
