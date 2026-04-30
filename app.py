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
10. /calendar/*   — V3 Project Manager Mode (Google Calendar scheduling)

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
from services import perplexity_client, claude_client, freshness, career, predigest, path_engine, sme, stay_ahead, career_simulator, resume, scheduler, calendar_client, notifications, embeddings, vector_index, content_classifier, drive_connector, work_items, team, rbac, audit_log, reports, skill_heatmap
from services.audit_log import audit_action, target_user, target_goal, target_path_step, target_resume_entry

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

    # ─── Vector index upsert (V3 semantic search) ─────────────────
    # Embed the content + push to vector_index. Stub mode uses local
    # cosine; live mode uses Voyage + Pinecone. Failures don't block.
    try:
        embed_text = " ".join(filter(None, [
            data.get("title", ""),
            data.get("ai_summary", ""),
            " ".join(data.get("skills", []) or []),
            " ".join(data.get("concepts_covered", []) or []),
        ])).strip()
        if embed_text:
            vec = embeddings.embed_text(embed_text)
            vector_index.upsert(data["content_id"], vec, {
                "title": data.get("title"),
                "source": data.get("source"),
                "source_url": data.get("source_url"),
                "content_type": data.get("content_type") or data.get("type"),
                "duration_minutes": data.get("duration_minutes"),
                "difficulty": data.get("difficulty"),
                "skills": data.get("skills") or [],
                "concepts_covered": data.get("concepts_covered") or [],
            })
    except Exception as exc:
        print(f"[/content/add] vector upsert failed: {exc}")

    # ─── Path Engine trigger: content_added ─────────────────────
    # Optional. Caller passes target_user_id + target_goal_id to attribute
    # the new content to a specific path. The high-relevance check is
    # downstream in the engine prompt — this just delivers the signal.
    path_update = None
    target_user = data.get("target_user_id")
    target_goal = data.get("target_goal_id")
    if target_user:
        try:
            target_goal = target_goal or path_engine.primary_goal_id(target_user)
            if target_goal:
                path_update = path_engine.recompute(
                    target_user, target_goal, "content_added",
                    {"content_id": data["content_id"], "title": data.get("title"),
                     "skills": data.get("skills", []), "concepts_covered": data.get("concepts_covered", [])},
                )
        except Exception as exc:
            path_update = {"error": f"path engine trigger failed: {exc}"}

    return jsonify({
        "status": "ok",
        "content_id": data["content_id"],
        "total_indexed": len(content_index),
        "vector_count": vector_index.count(),
        "path_update": path_update,
    })


@app.route("/content/semantic_search", methods=["POST"])
def content_semantic_search():
    """
    Semantic search over the vector index. Embeds the query, queries
    Pinecone (or in-memory cosine in stub mode), returns top-K hits with
    metadata + score.

    Body: { query: str, top_k?: int (default 5), filter?: dict }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"error": "query required"}), 400
    top_k = int(data.get("top_k", 5))
    filt = data.get("filter")

    vec = embeddings.embed_text(query)
    matches = vector_index.query(vec, top_k=top_k, filter=filt)
    return jsonify({
        "query": query,
        "matches": matches,
        "modes": {"embeddings": "live" if embeddings.is_live() else "stub",
                  "vector_index": "live" if vector_index.is_live() else "stub"},
        "top_k": top_k,
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


@app.route("/content/coverage", methods=["POST"])
def content_coverage():
    """
    Library module dashboard — aggregate stats over the indexed corpus.

    Returns: {
      total: int,
      vector_total: int (Pinecone or stub cosine store),
      by_source: { source_name: count, ... },
      by_skill: { skill_cluster: count, ... },     # top 20 by count
      by_difficulty: { beginner|intermediate|advanced: count },
      by_content_type: { doc|video|tutorial|reference|exercise: count },
      recent: [...last 5 indexed items, newest first]
    }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    by_source, by_skill, by_difficulty, by_type = {}, {}, {}, {}
    for item in content_index:
        src = item.get("source") or "unknown"
        by_source[src] = by_source.get(src, 0) + 1
        for s in (item.get("skills") or []):
            by_skill[s] = by_skill.get(s, 0) + 1
        diff = item.get("difficulty") or "unknown"
        by_difficulty[diff] = by_difficulty.get(diff, 0) + 1
        ctype = item.get("content_type") or item.get("type") or "doc"
        by_type[ctype] = by_type.get(ctype, 0) + 1

    top_skills = dict(sorted(by_skill.items(), key=lambda kv: -kv[1])[:20])
    recent = sorted(content_index, key=lambda c: c.get("indexed_at", ""), reverse=True)[:5]
    recent_summary = [
        {
            "content_id": c.get("content_id"),
            "title": c.get("title"),
            "source": c.get("source"),
            "source_url": c.get("source_url"),
            "skills": c.get("skills") or [],
            "difficulty": c.get("difficulty"),
            "duration_minutes": c.get("duration_minutes"),
            "indexed_at": c.get("indexed_at"),
        }
        for c in recent
    ]

    return jsonify({
        "total": len(content_index),
        "vector_total": vector_index.count(),
        "by_source": by_source,
        "by_skill": top_skills,
        "by_difficulty": by_difficulty,
        "by_content_type": by_type,
        "recent": recent_summary,
        "modes": {
            "embeddings": "live" if embeddings.is_live() else "stub",
            "vector_index": "live" if vector_index.is_live() else "stub",
            "drive": "live" if drive_connector.is_connected() else "stub",
        },
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

    # ─── Path Engine trigger: session_complete ───────────────────
    # If the caller passes path_step_id, mark that step done with the
    # captured mastery + actual duration before recompute. Otherwise just
    # recompute the user's primary path on the session signal.
    path_update = None
    try:
        step_id = data.get("path_step_id")
        goal_id = data.get("goal_id") or path_engine.find_step_owner(user_id, step_id) if step_id else (data.get("goal_id") or path_engine.primary_goal_id(user_id))
        avg_mastery = None
        if data.get("concepts"):
            scores = [c.get("confidence") for c in data["concepts"] if c.get("confidence") is not None]
            if scores:
                avg_mastery = sum(scores) / len(scores)
        if step_id and goal_id:
            path_engine.mark_step_done(user_id, goal_id, step_id, mastery=avg_mastery, duration_minutes=data.get("duration_minutes"))
        if goal_id:
            path_update = path_engine.recompute(
                user_id, goal_id, "session_complete",
                {"step_id": step_id, "session_title": data.get("session_title"), "gaps": data.get("gaps", []), "mastery": avg_mastery},
            )
    except Exception as exc:
        path_update = {"error": f"path engine trigger failed: {exc}"}

    return jsonify({
        "status": "ok",
        "concepts_saved": concepts_saved,
        "memories_added": memories_added,
        "path_update": path_update,
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

    # V3 — Project Manager Mode: upcoming Schedule_Blocks + just-fired nudges.
    # Lets ContextPanel render "Next learning block: Today 2:30 PM — Service Mesh"
    # and the chat composer surface conflict_pending blocks in the next greeting.
    now_iso = datetime.utcnow().isoformat()
    user_blocks = [b for b in SCHEDULE_BLOCKS if b["employee_id"] == user_id]
    upcoming = sorted(
        [b for b in user_blocks if b["status"] in ("scheduled", "rescheduled") and b["end_at"] > now_iso],
        key=lambda b: b["start_at"],
    )
    conflict_pending = [b for b in user_blocks if b["status"] == "conflict_pending"]
    recent_nudges = [n for n in NUDGE_LOG if n["employee_id"] == user_id][-5:]
    result["schedule"] = {
        "upcoming": upcoming[:5],
        "next_block_at": upcoming[0]["start_at"] if upcoming else None,
        "conflict_pending": conflict_pending,
        "recent_nudges": recent_nudges,
    }

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

    # ─── Path Engine trigger: staleness_flag ────────────────────
    # Only fire the engine on substantive/breaking changes. Caller passes
    # target_user_id + target_goal_id so the right path gets the refresher.
    path_update = None
    if should_notify:
        target_user = data.get("target_user_id") or context.get("user_id")
        target_goal = data.get("target_goal_id") or context.get("goal_id")
        if target_user:
            try:
                target_goal = target_goal or path_engine.primary_goal_id(target_user)
                if target_goal:
                    path_update = path_engine.recompute(
                        target_user, target_goal, "staleness_flag",
                        {"source_url": source_url, "category": category,
                         "summary": classification.get("summary", ""),
                         "affected_concepts": classification.get("affected_concepts", [])},
                    )
            except Exception as exc:
                path_update = {"error": f"path engine trigger failed: {exc}"}

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
        "path_update": path_update,
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


@app.route("/goal/create", methods=["POST"])
@audit_action(
    "goal:create",
    target_fn=target_goal,
    details_fn=lambda req, _resp: {"priority": ((req.get_json(silent=True) or {}).get("goal") or {}).get("priority")},
)
def goal_create():
    """
    Create a new goal + empty path. Body: { user_id, goal: {name, priority?,
    objective?, timeline?, success_criteria?, readiness?} }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_input = data.get("goal") or {}
    if not goal_input.get("name"):
        return jsonify({"error": "goal.name required"}), 400
    return jsonify(path_engine.create_goal(user_id, goal_input))


@app.route("/goal/archive", methods=["POST"])
@audit_action("goal:archive", target_fn=target_goal)
def goal_archive():
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_id = data.get("goal_id")
    if not goal_id:
        return jsonify({"error": "goal_id required"}), 400
    return jsonify(path_engine.archive_goal(user_id, goal_id))


@app.route("/goal/update_progress", methods=["POST"])
def goal_update_progress():
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_id = data.get("goal_id")
    if not goal_id:
        return jsonify({"error": "goal_id required"}), 400
    return jsonify(path_engine.update_goal_progress(
        user_id, goal_id,
        readiness=data.get("readiness"),
        delta=data.get("delta"),
    ))


@app.route("/path/reorder", methods=["POST"])
def path_reorder():
    """Manual learner edit — move a step. Marks the step inserted_by=learner."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_id = data.get("goal_id")
    step_id = data.get("step_id")
    new_order = data.get("new_order")
    if not (goal_id and step_id and new_order is not None):
        return jsonify({"error": "goal_id, step_id, new_order required"}), 400
    return jsonify(path_engine.reorder_step(user_id, goal_id, step_id, new_order))


@app.route("/path/skip_step", methods=["POST"])
@audit_action("path:skip_step", target_fn=target_path_step)
def path_skip_step():
    """Manual learner edit — skip a step. Engine never unskips."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_id = data.get("goal_id")
    step_id = data.get("step_id")
    if not (goal_id and step_id):
        return jsonify({"error": "goal_id, step_id required"}), 400
    return jsonify(path_engine.skip_step(user_id, goal_id, step_id, reason=data.get("reason", "")))


@app.route("/path/mark_done", methods=["POST"])
@audit_action(
    "path:mark_done",
    target_fn=target_path_step,
    details_fn=lambda req, _resp: {"mastery": (req.get_json(silent=True) or {}).get("mastery")},
)
def path_mark_done():
    """Mark a step done with optional mastery + duration. Used by capture flow + manual."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    goal_id = data.get("goal_id")
    step_id = data.get("step_id")
    if not (goal_id and step_id):
        return jsonify({"error": "goal_id, step_id required"}), 400
    return jsonify(path_engine.mark_step_done(
        user_id, goal_id, step_id,
        mastery=data.get("mastery"),
        duration_minutes=data.get("duration_minutes"),
    ))


@app.route("/assignment/create", methods=["POST"])
def assignment_create():
    """
    Manager assigns content into a learner's path. Triggers the Path
    Adjustment Engine with assignment_create — engine inserts the new
    step, marked inserted_by=manager.

    Body: { user_id (learner), goal_id?, manager?, title, source?, url?,
            estimated_minutes?, due_at? }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id")
    if not user_id or not data.get("title"):
        return jsonify({"error": "user_id and title required"}), 400
    goal_id = data.get("goal_id") or path_engine.primary_goal_id(user_id)
    if not goal_id:
        return jsonify({"error": f"no active goal for user {user_id} — create one first"}), 400
    path_engine.queue_assignment(user_id, {
        "title": data["title"],
        "source": data.get("source"),
        "url": data.get("url"),
        "estimated_minutes": data.get("estimated_minutes", 30),
        "manager": data.get("manager"),
        "due_at": data.get("due_at"),
    })
    result = path_engine.recompute(
        user_id, goal_id, "assignment_create",
        {"assignments": path_engine.drain_assignments(user_id)},
    )
    return jsonify(result)


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
@audit_action(
    "sme:book",
    target_fn=lambda req, _resp: f"sme:{(req.get_json(silent=True) or {}).get('sme_id', '?')}",
)
def sme_book():
    """
    Book a session with an SME.

    Two modes:
      A) Real slot booking — when start_at + end_at are provided, creates
         dual Google Calendar events (learner + SME) and writes a booking
         row matching Table 20.
      B) Legacy stub — when only `slot` (free-text) is provided, returns
         the V1 mock confirmation. Kept for backwards compat.
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    sme_id = data.get("sme_id")
    learner_id = data.get("learner_id", "demo-user")
    topic = data.get("topic", "")
    if not sme_id:
        return jsonify({"error": "sme_id required"}), 400
    start_at = data.get("start_at")
    end_at = data.get("end_at")
    if start_at and end_at:
        return jsonify(sme.book_slot_with_sme(
            sme_id=sme_id, learner_id=learner_id, topic=topic,
            start_at=start_at, end_at=end_at,
        ))
    slot = data.get("slot")
    return jsonify(sme.book_sme(sme_id=sme_id, learner_id=learner_id, topic=topic, slot=slot))


@app.route("/sme/find_slots", methods=["POST"])
def sme_find_slots():
    """
    Intersect an SME's schedule_window with the learner's busy calendar
    windows, return top-N candidate slots.

    Body: { sme_id, learner_id, duration_min?, count?, window_days? }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    sme_id = data.get("sme_id")
    learner_id = data.get("learner_id", "demo-user")
    if not sme_id:
        return jsonify({"error": "sme_id required"}), 400
    return jsonify(sme.find_slots_for_sme(
        sme_id=sme_id,
        learner_id=learner_id,
        duration_min=int(data.get("duration_min", 30)),
        count=int(data.get("count", 3)),
        window_days=int(data.get("window_days", 14)),
    ))


@app.route("/sme/bookings", methods=["POST"])
def sme_bookings():
    """List a learner's bookings (legacy — single side). Use /sme/my_bookings for both sides."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    learner_id = data.get("learner_id", "demo-user")
    return jsonify(sme.list_bookings(learner_id=learner_id))


@app.route("/sme/my_bookings", methods=["POST"])
def sme_my_bookings():
    """
    Unified bookings inbox — both sides in one call.
    Body: { user_id, include_past?: bool=false }
    Returns: { as_learner: [...], as_sme: [...], counts, total }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    return jsonify(sme.list_my_bookings(
        user_id=user_id,
        include_past=bool(data.get("include_past", False)),
    ))


@app.route("/sme/register", methods=["POST"])
def sme_register():
    """
    Self-registration as an SME. Body:
      { employee_id, profile: { name (req), subjects (list, req),
        subject_mastery, schedule_window, timezone, languages,
        rate_model, rate_per_30min?, rate_currency?,
        expectations_from_students, bio,
        preferred_session_length, sme_type? } }
    Idempotent on employee_id — re-registering updates the profile.
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    employee_id = data.get("employee_id")
    profile = data.get("profile", {}) or {}
    if not employee_id:
        return jsonify({"error": "employee_id required"}), 400
    return jsonify(sme.register_sme(employee_id=employee_id, profile=profile))


@app.route("/sme/profile", methods=["POST"])
def sme_profile():
    """Return an SME's own profile (for the edit form). Body: { employee_id }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    employee_id = data.get("employee_id")
    if not employee_id:
        return jsonify({"error": "employee_id required"}), 400
    return jsonify(sme.get_sme_profile(employee_id=employee_id))


@app.route("/sme/list", methods=["POST"])
def sme_list():
    """Browse the SME marketplace. Body: { active_only?: bool=true, limit?: int=100 }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    return jsonify(sme.list_smes(
        active_only=bool(data.get("active_only", True)),
        limit=int(data.get("limit", 100)),
    ))


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
# V3 — Resume Module (living service record + job-tailored resume)
# ─────────────────────────────────────────────

@app.route("/resume/add", methods=["POST"])
def resume_add():
    """
    Capture a journal entry. Two modes:
      - Conversational: { user_id, raw_input } → Claude extracts structured fields
      - Direct: { user_id, structured: {...} } → caller-provided structure

    Optional social fields (in `structured` or top-level):
      company, project, peers_to_share_with: [emails], peers_to_endorse: [emails]
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    raw = data.get("raw_input", "")
    structured = data.get("structured") or {}
    # Hoist social fields onto structured if passed top-level
    for k in ("company", "project", "peers_to_share_with", "peers_to_endorse"):
        if data.get(k) is not None and structured.get(k) is None:
            structured[k] = data[k]
    if not raw and not (structured and (structured.get("title") or structured.get("description") or structured.get("company"))):
        return jsonify({"error": "raw_input or structured required"}), 400
    return jsonify(resume.add_entry(user_id=user_id, raw_input=raw, structured=structured))


@app.route("/resume/share", methods=["POST"])
@audit_action(
    "resume:share",
    target_fn=target_resume_entry,
    details_fn=lambda req, _resp: {"recipients": len((req.get_json(silent=True) or {}).get("peer_emails") or [])},
)
def resume_share():
    """Share an existing entry with peers. Body: { user_id, entry_id, peer_emails: [str] }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    entry_id = data.get("entry_id")
    peer_emails = data.get("peer_emails") or []
    if not entry_id:
        return jsonify({"error": "entry_id required"}), 400
    return jsonify(resume.share_entry(user_id=user_id, entry_id=entry_id, peer_emails=peer_emails))


@app.route("/resume/request_endorsements", methods=["POST"])
def resume_request_endorsements():
    """Ask peers to endorse an entry. Body: { user_id, entry_id, peer_emails }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    entry_id = data.get("entry_id")
    peer_emails = data.get("peer_emails") or []
    if not entry_id:
        return jsonify({"error": "entry_id required"}), 400
    return jsonify(resume.request_endorsements(user_id=user_id, entry_id=entry_id, peer_emails=peer_emails))


@app.route("/resume/endorse", methods=["POST"])
@audit_action("resume:endorse", target_fn=target_resume_entry)
def resume_endorse():
    """
    Peer endorses an entry. Body:
      { author_user_id, entry_id, endorser_email, endorser_name?, endorser_role?, comment? }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    return jsonify(resume.endorse_entry(
        author_user_id=data.get("author_user_id"),
        entry_id=data.get("entry_id"),
        endorser_email=data.get("endorser_email"),
        endorser_name=data.get("endorser_name", ""),
        endorser_role=data.get("endorser_role", ""),
        comment=data.get("comment", ""),
    ))


@app.route("/resume/decline_endorsement", methods=["POST"])
def resume_decline_endorsement():
    """Peer declines an endorsement request. Body: { author_user_id, entry_id, endorser_email, reason? }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    return jsonify(resume.decline_endorsement(
        author_user_id=data.get("author_user_id"),
        entry_id=data.get("entry_id"),
        endorser_email=data.get("endorser_email"),
        reason=data.get("reason", ""),
    ))


@app.route("/resume/feed", methods=["POST"])
def resume_feed():
    """Activity feed for a user (by email). Body: { user_email, limit?: int=25 }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    return jsonify(resume.get_feed(user_email=data.get("user_email", ""), limit=int(data.get("limit", 25))))


@app.route("/resume/journal", methods=["POST"])
def resume_journal():
    """List a user's full journal."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    limit = int(data.get("limit", 50))
    return jsonify(resume.list_journal(user_id=user_id, limit=limit))


@app.route("/resume/tailor", methods=["POST"])
def resume_tailor():
    """
    The killer feature. Given a job posting URL (or pasted text),
    return a tailored resume drawing from the user's journal.
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    job_url = data.get("job_url", "").strip()
    job_description = data.get("job_description", "").strip()
    if not job_url and not job_description:
        return jsonify({"error": "job_url or job_description required"}), 400
    return jsonify(resume.tailor_resume(user_id=user_id, job_url=job_url, job_description=job_description))


# ─────────────────────────────────────────────
# V3 — Drive connector (Workspace training-content ingest)
# Walks Drive (live or stub), classifies each file via Claude (or keyword
# stub), embeds, upserts to vector_index, appends to content_index.
# ─────────────────────────────────────────────

@app.route("/drive/index", methods=["POST"])
def drive_index():
    """
    One-shot ingest run.

    Body: { folder_id?: str, query?: str, limit?: int (default 25),
            target_user_id?: str (for path-engine content_added trigger),
            target_goal_id?: str }

    Returns: { ingested: [...], failed: [...], counts: {...},
               modes: {drive, classifier, embeddings, vector_index} }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    folder_id = data.get("folder_id")
    query = data.get("query")
    limit = int(data.get("limit", 25))
    target_user = data.get("target_user_id")
    target_goal = data.get("target_goal_id")

    files = drive_connector.list_training_files(query=query, folder_id=folder_id, limit=limit)
    ingested = []
    failed = []
    for f in files:
        try:
            text = drive_connector.fetch_file_text(f["file_id"], f["mime_type"])
            classification = content_classifier.classify_content(
                title=f["title"], text=text, source="google-drive", source_url=f.get("url", ""),
            )
            content_item = {
                "content_id": f"drive-{f['file_id']}",
                "title": f["title"],
                "source": "google-drive",
                "source_url": f.get("url"),
                "content_type": classification.get("content_type", "doc"),
                "duration_minutes": classification.get("duration_minutes_estimate", 5),
                "difficulty": classification.get("difficulty", "intermediate"),
                "skills": classification.get("skills", []),
                "concepts_covered": classification.get("concepts_covered", []),
                "prerequisites": classification.get("prerequisites", []),
                "ai_summary": classification.get("summary", ""),
                "quality_score": classification.get("quality_score", 0.6),
                "owner": f.get("owner"),
                "modified_time": f.get("modified_time"),
                "indexed_at": datetime.utcnow().isoformat(),
                "_classifier_mode": classification.get("_mode"),
            }
            # Skip duplicates (re-runs of /drive/index on same files)
            content_index[:] = [c for c in content_index if c.get("content_id") != content_item["content_id"]]
            content_index.append(content_item)

            # Embed + index
            embed_text_blob = " ".join(filter(None, [
                content_item["title"], content_item["ai_summary"],
                " ".join(content_item["skills"]),
                " ".join(content_item["concepts_covered"]),
                text[:2000],
            ]))
            vec = embeddings.embed_text(embed_text_blob)
            vector_index.upsert(content_item["content_id"], vec, {
                "title": content_item["title"], "source": content_item["source"],
                "source_url": content_item["source_url"],
                "content_type": content_item["content_type"],
                "duration_minutes": content_item["duration_minutes"],
                "difficulty": content_item["difficulty"],
                "skills": content_item["skills"],
                "concepts_covered": content_item["concepts_covered"],
            })
            ingested.append({"content_id": content_item["content_id"], "title": content_item["title"],
                             "skills": content_item["skills"], "difficulty": content_item["difficulty"]})
        except Exception as exc:
            failed.append({"file_id": f.get("file_id"), "title": f.get("title"), "error": str(exc)})

    # Path Engine trigger: content_added (one batch trigger per recompute,
    # not per file — avoids 25 separate recompute calls)
    path_update = None
    if target_user and ingested:
        try:
            target_goal = target_goal or path_engine.primary_goal_id(target_user)
            if target_goal:
                path_update = path_engine.recompute(
                    target_user, target_goal, "content_added",
                    {"batch_size": len(ingested), "sample_titles": [i["title"] for i in ingested[:3]]},
                )
        except Exception as exc:
            path_update = {"error": f"path engine trigger failed: {exc}"}

    return jsonify({
        "ingested": ingested,
        "failed": failed,
        "counts": {"ingested": len(ingested), "failed": len(failed),
                   "content_index_total": len(content_index),
                   "vector_index_total": vector_index.count()},
        "modes": {
            "drive": "live" if drive_connector.is_connected() else "stub",
            "classifier": "live" if content_classifier.is_live() else "stub",
            "embeddings": "live" if embeddings.is_live() else "stub",
            "vector_index": "live" if vector_index.is_live() else "stub",
        },
        "path_update": path_update,
    })


# ─────────────────────────────────────────────
# V3 — Project Manager Mode (Calendar scheduling)
# Solution Arch §14.A. Endpoints: /calendar/find_slots, /calendar/book,
# /calendar/reschedule, /calendar/cancel.
#
# Phase 1 storage: in-memory SCHEDULE_BLOCKS list (Schedule_Blocks Table 18
# schema). Phase 2: migrate to Airtable. Phase B (next session): wire real
# Google Calendar OAuth — calendar_client returns stub busy windows for now.
# ─────────────────────────────────────────────

SCHEDULE_BLOCKS = []  # Phase 1 in-memory store; Phase 2 → Airtable Table 18
NUDGE_LOG = []         # Phase 1 in-memory log of dispatched 5-min-prior nudges
_BLOCK_ID_COUNTER = [0]


def _next_block_id():
    _BLOCK_ID_COUNTER[0] += 1
    return _BLOCK_ID_COUNTER[0]


def _parse_iso(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


@app.route("/calendar/find_slots", methods=["POST"])
def calendar_find_slots():
    """
    Find candidate learning slots in the learner's calendar.

    Body:
      { user_id, duration_min: 30, count: 3,
        window_start?: ISO, window_end?: ISO,
        rhythm?: "morning"|"afternoon"|"evening"|"default" }

    Returns: { slots: [{day, time, fit, start, end, score}, ...],
               connected: bool, rhythm: str }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    duration_min = int(data.get("duration_min", 30))
    count = int(data.get("count", 3))
    rhythm = data.get("rhythm", "default")
    window_start = _parse_iso(data.get("window_start"))
    window_end = _parse_iso(data.get("window_end"))
    goals = data.get("goals")  # V3 multi-goal: optional list of {goal_id, name, priority, deadline, progress_pct}

    slots = scheduler.compute_free_slots(
        user_id=user_id,
        duration_min=duration_min,
        count=count,
        window_start=window_start,
        window_end=window_end,
        rhythm=rhythm,
        goals=goals,
    )
    response = {
        "slots": slots,
        "connected": calendar_client.is_connected(),
        "rhythm": rhythm,
        "duration_min": duration_min,
    }
    if goals:
        response["goal_budget"] = scheduler.compute_goal_budget(goals)
    return jsonify(response)


@app.route("/calendar/goal_budget", methods=["POST"])
def calendar_goal_budget():
    """
    Goals Dashboard — return weighted minutes-per-week per goal.
    Body: { goals: [{goal_id, name, priority, deadline, progress_pct}],
            total_minutes_per_week?: 300 }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    goals = data.get("goals", [])
    total = int(data.get("total_minutes_per_week", 300))
    return jsonify({"budget": scheduler.compute_goal_budget(goals, total_minutes_per_week=total)})


@app.route("/calendar/book", methods=["POST"])
def calendar_book():
    """
    Book a slot. Creates a Calendar event + Schedule_Blocks row + schedules
    the 5-min-prior nudge.

    Body:
      { user_id, path_step_id?, step_title, start_at: ISO, end_at: ISO,
        description? }

    Returns: { block_id, calendar_event_id, calendar_event_url, scheduled_at,
               nudge_at, status }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    path_step_id = data.get("path_step_id")
    step_title = data.get("step_title", "Learning session")
    start_at = _parse_iso(data.get("start_at"))
    end_at = _parse_iso(data.get("end_at"))
    description = data.get("description", "")
    if not start_at or not end_at:
        return jsonify({"error": "start_at and end_at required (ISO 8601)"}), 400

    event = calendar_client.insert_event(
        user_id=user_id,
        title=f"📚 Aasan — {step_title}",
        start=start_at,
        end=end_at,
        description=description or f"Aasan learning session: {step_title}",
    )

    block = {
        "block_id": _next_block_id(),
        "employee_id": user_id,
        "path_step_id": path_step_id,
        "step_title": step_title,
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "duration_minutes": int((end_at - start_at).total_seconds() // 60),
        "calendar_event_id": event["event_id"],
        "calendar_event_url": event["event_url"],
        "status": "scheduled",
        "nudge_at": (start_at - timedelta(minutes=5)).isoformat(),
        "nudge_sent_at": None,
        "reschedule_count": 0,
        "original_start_at": start_at.isoformat(),
        "created_at": datetime.utcnow().isoformat(),
        "mode": event.get("mode", "live"),
    }
    SCHEDULE_BLOCKS.append(block)

    return jsonify({
        "block_id": block["block_id"],
        "calendar_event_id": block["calendar_event_id"],
        "calendar_event_url": block["calendar_event_url"],
        "scheduled_at": block["start_at"],
        "nudge_at": block["nudge_at"],
        "status": block["status"],
        "mode": block["mode"],
    })


@app.route("/calendar/reschedule", methods=["POST"])
def calendar_reschedule():
    """
    Two modes:
      1. Walk-mode: { user_id } → returns blocks that conflict with
         freshly-fetched busy windows (used by daily cron). Does NOT push;
         conflicts are surfaced in next chat session.
      2. Move-mode: { block_id, new_start_at, new_end_at } → moves the event.

    Returns: { conflicts: [...], moved?: {block_id, ...} }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}

    block_id = data.get("block_id")
    if block_id is not None:
        new_start = _parse_iso(data.get("new_start_at"))
        new_end = _parse_iso(data.get("new_end_at"))
        if not new_start or not new_end:
            return jsonify({"error": "new_start_at and new_end_at required"}), 400
        block = next((b for b in SCHEDULE_BLOCKS if b["block_id"] == block_id), None)
        if not block:
            return jsonify({"error": f"block {block_id} not found"}), 404
        calendar_client.delete_event(block["employee_id"], block["calendar_event_id"])
        event = calendar_client.insert_event(
            user_id=block["employee_id"],
            title=f"📚 Aasan — {block['step_title']}",
            start=new_start,
            end=new_end,
        )
        block["start_at"] = new_start.isoformat()
        block["end_at"] = new_end.isoformat()
        block["calendar_event_id"] = event["event_id"]
        block["calendar_event_url"] = event["event_url"]
        block["nudge_at"] = (new_start - timedelta(minutes=5)).isoformat()
        block["nudge_sent_at"] = None
        block["reschedule_count"] = block.get("reschedule_count", 0) + 1
        block["status"] = "rescheduled"
        return jsonify({"moved": block, "conflicts": []})

    # Walk mode: detect conflicts for this user's active blocks
    user_id = data.get("user_id", "demo-user")
    active = [b for b in SCHEDULE_BLOCKS if b["employee_id"] == user_id and b["status"] in ("scheduled", "rescheduled")]
    if not active:
        return jsonify({"conflicts": []})
    starts = [_parse_iso(b["start_at"]) for b in active]
    ends = [_parse_iso(b["end_at"]) for b in active]
    busy = calendar_client.list_busy_windows(user_id, min(starts), max(ends) + timedelta(hours=1))
    conflicts = scheduler.detect_conflicts(active, busy)
    return jsonify({"conflicts": conflicts})


@app.route("/calendar/cancel", methods=["POST"])
def calendar_cancel():
    """
    Cancel a booked block. Deletes the Calendar event and marks the
    Schedule_Block as cancelled.

    Body: { block_id }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    block_id = data.get("block_id")
    if block_id is None:
        return jsonify({"error": "block_id required"}), 400
    block = next((b for b in SCHEDULE_BLOCKS if b["block_id"] == block_id), None)
    if not block:
        return jsonify({"error": f"block {block_id} not found"}), 404
    calendar_client.delete_event(block["employee_id"], block["calendar_event_id"])
    block["status"] = "cancelled"
    return jsonify({"ok": True, "block": block})


@app.route("/calendar/blocks", methods=["POST"])
def calendar_blocks():
    """List a learner's active Schedule_Blocks. Used by Goals Dashboard."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    include_past = bool(data.get("include_past", False))
    blocks = [b for b in SCHEDULE_BLOCKS if b["employee_id"] == user_id]
    if not include_past:
        blocks = [b for b in blocks if b["status"] in ("scheduled", "rescheduled")]
    return jsonify({"blocks": blocks, "count": len(blocks)})


# ─────────────────────────────────────────────
# V3 — Calendar cron endpoints
# Render Cron Jobs hit these on schedule. Both are idempotent and safe to
# re-run; both return summary JSON for monitoring.
#
# Suggested schedules (configure in Render dashboard or render.yaml):
#   /cron/calendar_nudges  — every 1 minute (catches each block's 5-min-prior window)
#   /cron/calendar_walk    — once per hour (rescans all active blocks for new conflicts)
# ─────────────────────────────────────────────

@app.route("/cron/calendar_nudges", methods=["POST", "GET"])
def cron_calendar_nudges():
    """
    Dispatch 5-min-prior nudges. Scans SCHEDULE_BLOCKS for blocks where
    nudge_at <= now AND nudge_sent_at IS NULL AND status in (scheduled, rescheduled).
    For each, marks nudge_sent_at and appends a notification log row.

    Phase B-this-session: log only (notification dispatcher is stubbed —
    real email/web push is Phase C).
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    now = datetime.utcnow()
    dispatched = []
    for block in SCHEDULE_BLOCKS:
        if block["status"] not in ("scheduled", "rescheduled"):
            continue
        if block.get("nudge_sent_at"):
            continue
        nudge_at = datetime.fromisoformat(block["nudge_at"].replace("Z", "+00:00")).replace(tzinfo=None)
        if nudge_at > now:
            continue
        # Skip nudges that are stale (more than 30 min past) — start time has come and gone
        start_at = datetime.fromisoformat(block["start_at"].replace("Z", "+00:00")).replace(tzinfo=None)
        if (now - start_at).total_seconds() > 1800:
            block["status"] = "missed"
            continue
        block["nudge_sent_at"] = now.isoformat()
        nudge_payload = {
            "block_id": block["block_id"],
            "employee_id": block["employee_id"],
            "step_title": block["step_title"],
            "start_at": block["start_at"],
            "dispatched_at": now.isoformat(),
        }
        # Phase C: real dispatch via notifications.py (Gmail + Slack).
        # Returns per-channel {ok, mode, error?} — failures don't block the
        # nudge_sent_at marker; they're recorded for observability.
        channel_results = notifications.dispatch_nudge(nudge_payload)
        NUDGE_LOG.append({
            **nudge_payload,
            "channels": channel_results,
            "channel": "+".join(c for c, r in channel_results.items() if r.get("ok")) or "log",
        })
        dispatched.append(block["block_id"])

    return jsonify({
        "dispatched": dispatched,
        "count": len(dispatched),
        "scanned": len(SCHEDULE_BLOCKS),
        "ran_at": now.isoformat(),
    })


@app.route("/cron/calendar_walk", methods=["POST", "GET"])
def cron_calendar_walk():
    """
    Daily reschedule walk. For every user with active blocks, refetch their
    Calendar busy windows and detect blocks that now overlap a meeting that
    wasn't there at booking time. Marks block.status = 'conflict_pending'
    so the next chat session can surface a reschedule prompt.

    Does NOT push notifications and does NOT auto-reschedule. The learner
    sees the conflict in their next greeting and chooses.
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401

    by_user = {}
    for b in SCHEDULE_BLOCKS:
        if b["status"] in ("scheduled", "rescheduled"):
            by_user.setdefault(b["employee_id"], []).append(b)

    flagged = []
    for user_id, blocks in by_user.items():
        starts = [datetime.fromisoformat(b["start_at"].replace("Z", "+00:00")) for b in blocks]
        ends = [datetime.fromisoformat(b["end_at"].replace("Z", "+00:00")) for b in blocks]
        if not starts:
            continue
        busy = calendar_client.list_busy_windows(user_id, min(starts), max(ends) + timedelta(hours=1))
        conflicts = scheduler.detect_conflicts(blocks, busy)
        for c in conflicts:
            target = next((b for b in SCHEDULE_BLOCKS if b["block_id"] == c["block_id"]), None)
            if target and target["status"] != "conflict_pending":
                target["status"] = "conflict_pending"
                target["conflict_with"] = c.get("conflict_with")
                flagged.append(target["block_id"])

    return jsonify({
        "flagged": flagged,
        "count": len(flagged),
        "users_scanned": len(by_user),
        "ran_at": datetime.utcnow().isoformat(),
    })


@app.route("/calendar/nudges", methods=["POST"])
def calendar_nudges():
    """
    Read endpoint — frontend calls this on context load to display
    'just dispatched' nudges (the ones the cron fired since last load).
    Returns the most-recent N for the user.
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    user_id = data.get("user_id", "demo-user")
    limit = int(data.get("limit", 10))
    user_nudges = [n for n in NUDGE_LOG if n["employee_id"] == user_id]
    return jsonify({"nudges": user_nudges[-limit:], "count": len(user_nudges)})


# ─────────────────────────────────────────────
# V3 — RBAC + Admin Console (Internal Pilot Pack · Phase A)
# Identity model: actor user_id pulled from X-Aasan-User header or
# JSON body. Phase 2 will parse Clerk JWTs; Phase 1 trusts client.
# ─────────────────────────────────────────────

@app.route("/admin/me", methods=["POST", "GET"])
def admin_me():
    """Active user's identity + role + module visibility for UI gating."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    user_id = rbac.get_actor_user_id(request)
    return jsonify(rbac.me(user_id))


@app.route("/admin/users/list", methods=["POST"])
def admin_users_list():
    """List users in the org. Body: { filter_role?, search?, limit? }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "admin:users"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    return jsonify(rbac.list_users(
        filter_role=data.get("filter_role"),
        search=data.get("search"),
        limit=int(data.get("limit", 200)),
    ))


@app.route("/admin/users/set_role", methods=["POST"])
@audit_action(
    "admin:role_change",
    target_fn=target_user,
    details_fn=lambda req, _resp: {"new_role": (req.get_json(silent=True) or {}).get("role")},
)
def admin_users_set_role():
    """Body: { target_user_id, role }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "admin:users"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    target = data.get("target_user_id")
    new_role = data.get("role")
    if not (target and new_role):
        return jsonify({"error": "target_user_id and role required"}), 400
    return jsonify(rbac.set_role(actor, target, new_role))


@app.route("/admin/users/import_csv", methods=["POST"])
@audit_action(
    "admin:user_bulk_import",
    target_fn=lambda _req, _resp: "users:csv_import",
    details_fn=lambda req, _resp: {"csv_lines": len((req.get_json(silent=True) or {}).get("csv", "").splitlines())},
)
def admin_users_import_csv():
    """Bulk import users from CSV. Body: { csv: str } — pasted CSV content."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "admin:users"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    csv_text = data.get("csv", "")
    return jsonify(rbac.import_users_csv(actor, csv_text))


@app.route("/admin/reports/list", methods=["POST"])
def admin_reports_list():
    """List available reports + descriptions."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "report:run"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    return jsonify(reports.list_reports())


@app.route("/admin/reports/run", methods=["POST"])
@audit_action(
    "report:run",
    target_fn=lambda req, _resp: f"report:{(req.get_json(silent=True) or {}).get('report_id', '?')}",
    details_fn=lambda req, _resp: {"filters": (req.get_json(silent=True) or {}).get("filters") or {}},
)
def admin_reports_run():
    """Run a report. Body: { report_id, filters? }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "report:run"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    report_id = data.get("report_id")
    if not report_id:
        return jsonify({"error": "report_id required"}), 400
    return jsonify(reports.run(report_id, data.get("filters") or {}))


@app.route("/admin/reports/export_csv", methods=["POST"])
@audit_action(
    "report:export",
    target_fn=lambda req, _resp: f"report:{(req.get_json(silent=True) or {}).get('report_id', '?')}",
)
def admin_reports_export_csv():
    """Export a report as CSV. Body: { report_id, filters? }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "report:export"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    report_id = data.get("report_id")
    if not report_id:
        return jsonify({"error": "report_id required"}), 400
    csv_text = reports.export_csv(report_id, data.get("filters") or {})
    return jsonify({
        "csv": csv_text,
        "filename": f"aasan-report-{report_id}-{datetime.utcnow().date().isoformat()}.csv",
    })


@app.route("/admin/skill_heatmap", methods=["POST"])
@audit_action("report:skill_heatmap", target_fn=lambda req, _resp: "report:skill_heatmap")
def admin_skill_heatmap():
    """
    Org-level skill heatmap. Body: { departments_filter?: [str] }
    Returns matrix[dept][skill] + supply (content + SMEs) + demand-supply gaps.
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "report:run"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    return jsonify(skill_heatmap.build_heatmap(
        departments_filter=data.get("departments_filter") or None,
    ))


@app.route("/admin/audit_log", methods=["POST"])
def admin_audit_log():
    """
    Search the audit log. org_admin only. Body filters all optional:
      filter_actor, filter_action (supports trailing-* glob), filter_target,
      since (ISO), until (ISO), search (full-text), limit (default 200).
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "admin:audit_log"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    return jsonify(audit_log.query(
        filter_actor=data.get("filter_actor"),
        filter_action=data.get("filter_action"),
        filter_target=data.get("filter_target"),
        since=data.get("since"),
        until=data.get("until"),
        search=data.get("search"),
        limit=int(data.get("limit", 200)),
    ))


@app.route("/admin/audit_log/export_csv", methods=["POST"])
def admin_audit_log_export_csv():
    """Return CSV of (filtered) audit entries. org_admin only."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "admin:audit_log"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    csv_text = audit_log.export_csv(filters={
        "filter_actor":  data.get("filter_actor"),
        "filter_action": data.get("filter_action"),
        "filter_target": data.get("filter_target"),
        "since":         data.get("since"),
        "until":         data.get("until"),
        "search":        data.get("search"),
    })
    return jsonify({"csv": csv_text, "filename": f"aasan-audit-log-{datetime.utcnow().date().isoformat()}.csv"})


@app.route("/admin/users/csv_sample", methods=["GET", "POST"])
def admin_users_csv_sample():
    """Return a sample CSV body the admin can use as a template."""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    return jsonify({
        "csv": rbac.CSV_SAMPLE,
        "header_columns": ["email (required)", "name", "role", "department", "manager_email", "is_active"],
        "valid_roles": sorted(rbac.VALID_ROLES),
    })


@app.route("/admin/users/update", methods=["POST"])
@audit_action(
    "admin:user_update",
    target_fn=target_user,
    details_fn=lambda req, _resp: {"fields": list(((req.get_json(silent=True) or {}).get("fields") or {}).keys())},
)
def admin_users_update():
    """Body: { target_user_id, fields: {name?, email?, department?, manager_user_id?, is_active?} }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    actor = rbac.get_actor_user_id(request)
    if not rbac.has_any_permission(actor, "admin:users"):
        return jsonify({"error": "forbidden", "your_role": rbac.get_role(actor)}), 403
    data = request.json or {}
    target = data.get("target_user_id")
    fields = data.get("fields") or {}
    if not target:
        return jsonify({"error": "target_user_id required"}), 400
    return jsonify(rbac.update_user(actor, target, fields))


# ─────────────────────────────────────────────
# V3 — Team (manager view of team learning progress)
# Phase 1 storage: hardcoded demo team for `demo-user` manager.
# Phase D: real org structure via Workspace Directory API or HRIS.
# ─────────────────────────────────────────────

@app.route("/team/list", methods=["POST"])
def team_list():
    """Manager's direct reports + summary stats. Body: { manager_id, include_skip?: bool=false }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    manager_id = data.get("manager_id", "demo-user")
    include_skip = bool(data.get("include_skip", False))
    return jsonify(team.list_team(manager_id=manager_id, include_skip=include_skip))


@app.route("/team/org_chart", methods=["POST"])
def team_org_chart():
    """Manager → reports tree. Body: { root_user_id?: str }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    return jsonify(team.get_org_chart(root_user_id=data.get("root_user_id")))


@app.route("/team/member", methods=["POST"])
def team_member():
    """Detailed view of one report. Body: { manager_id, member_id }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    manager_id = data.get("manager_id", "demo-user")
    member_id = data.get("member_id")
    if not member_id:
        return jsonify({"error": "member_id required"}), 400
    return jsonify(team.get_team_member(manager_id=manager_id, member_id=member_id))


@app.route("/team/kudos", methods=["POST"])
@audit_action(
    "team:kudos",
    target_fn=lambda req, _resp: f"user:{(req.get_json(silent=True) or {}).get('report_id', '?')}",
)
def team_kudos():
    """Manager sends kudos to a report. Body: { manager_id, report_id, message? }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    return jsonify(team.send_kudos(
        manager_id=data.get("manager_id", "demo-user"),
        report_id=data.get("report_id"),
        message=data.get("message", ""),
    ))


@app.route("/team/kudos_sent", methods=["POST"])
def team_kudos_sent():
    """Manager's log of kudos sent. Body: { manager_id }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    return jsonify(team.list_kudos_sent(manager_id=data.get("manager_id", "demo-user")))


# ─────────────────────────────────────────────
# V3 — Work_Items (granular build-task tracker)
# Companion to JOURNAL.md. JOURNAL.md = narrative ship log (~10/quarter).
# Work_Items = granular tasks (~hundreds), with status transitions.
# Phase 1 in-memory; Phase 2 → Airtable Table 26.
# ─────────────────────────────────────────────

@app.route("/work_item/create", methods=["POST"])
def work_item_create():
    """
    Body: { title (required), status?, description?, owner?, parent_ship_date?,
            tags?: list[str], estimated_minutes?, actual_minutes? }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    title = data.pop("title", None)
    if not title:
        return jsonify({"error": "title required"}), 400
    return jsonify(work_items.create(title, **data))


@app.route("/work_item/update", methods=["POST"])
def work_item_update():
    """Body: { work_item_id, ...fields }"""
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    item_id = data.pop("work_item_id", None)
    if item_id is None:
        return jsonify({"error": "work_item_id required"}), 400
    return jsonify(work_items.update(item_id, **data))


@app.route("/work_item/get", methods=["POST"])
def work_item_get():
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    item_id = data.get("work_item_id")
    if item_id is None:
        return jsonify({"error": "work_item_id required"}), 400
    return jsonify(work_items.get(item_id))


@app.route("/work_item/list", methods=["POST"])
def work_item_list():
    """
    Body: { status?, tag?, owner?, parent_ship_date?, limit?: int=100,
            include_deleted?: bool=false }
    """
    if not verify_secret(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.json or {}
    return jsonify(work_items.list_items(
        status=data.get("status"),
        tag=data.get("tag"),
        owner=data.get("owner"),
        parent_ship_date=data.get("parent_ship_date"),
        limit=int(data.get("limit", 100)),
        include_deleted=bool(data.get("include_deleted", False)),
    ))


# ─────────────────────────────────────────────
# Run
# ─────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
