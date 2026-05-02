"""
Content Index — the unified catalog of all learning content.

This is V2 Data Model Section 2 Table 03, finally landed. Mirrors the
schema in `migrations/0005_content_index.sql`. Same Tier 0 dual-mode
pattern as path_engine / resume / work_items / audit_log:

  • Postgres-backed when SUPABASE_DB_URL is set
  • In-memory fallback otherwise (so local dev + demo without Supabase
    still works)

Public surface:

  upsert(row, actor=None) → dict
      Insert or update one row keyed on (source, external_id). Synthesizes
      external_id from slug(source+title) when missing. After insert,
      kicks off Voyage embedding + Pinecone upsert in best-effort mode
      (failures don't roll back the row).

  import_csv(csv_text, actor=None) → dict
      Parses CSV with csv.DictReader, normalizes column names (case-
      insensitive, trims), validates required fields, calls upsert() per
      row. Returns:
        {rows_processed, inserted, updated, embedded, errors:[{row,msg}]}

  list_for_browse(filters, limit, offset) → dict
      Paginated catalog browse. Filters: source, difficulty, is_free,
      content_type, search (substring match across title+description+
      skills+source), needs_embedding (Pinecone backfill).

  retrieve(query_text, top_k=30, filters=None) → list[dict]
      RAG retrieval. Embeds query → Pinecone search → enriches with
      content_index row data. Returns sorted by similarity score. Falls
      back to keyword search when Pinecone isn't available.

  delete_one(content_id) → dict
      Soft-aware: removes from Pinecone first, then deletes the row.

  embed_one(row) → str | None
      Compose embedding text from the row, call Voyage, upsert into
      Pinecone with content_id as the vector ID, return the embedding_id.

  embed_pending(limit=200) → dict
      Batch-embed any rows where embedding_id IS NULL.

  load_seed(seed_path) → dict
      Convenience: reads seed_data/aasan_content_seed_v1.csv from disk
      and runs import_csv. Used by /admin/content/load_seed during
      demo bootstrap.

CSV column expectations (per the canonical seed file):
  Required:  external_id · source · title · source_url · content_type
  Strongly:  duration_minutes · description · skills · prerequisites ·
             difficulty · is_free · language

skills + prerequisites are comma-separated in the CSV (e.g.
"langchain,rag,vector-stores"); stored as text[] in Postgres / list in
fallback. is_free accepts true/false/yes/no/1/0 (case-insensitive).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from . import db, embeddings, vector_index

logger = logging.getLogger(__name__)

# In-memory fallback store: { content_id: row }
_FALLBACK: dict[int, dict] = {}
_FALLBACK_BY_KEY: dict[tuple, int] = {}  # (source, external_id) → content_id
_FALLBACK_ID_COUNTER = [0]

# Allowed enum values
_VALID_CONTENT_TYPES = {"course", "video", "article", "lab", "quiz", "pdf",
                         "book", "slides", "interactive", "other"}
_VALID_DIFFICULTIES = {"beginner", "intermediate", "advanced", "expert"}

REQUIRED_COLUMNS = ["external_id", "source", "title", "source_url", "content_type"]
SOFT_COLUMNS = ["duration_minutes", "description", "skills", "prerequisites",
                "difficulty", "is_free", "language"]


# ──────────────────────────────────────────────────────────────
# Public surface
# ──────────────────────────────────────────────────────────────

def upsert(row: dict, actor: str | None = None) -> dict:
    """
    Insert or update one row. Returns the persisted row including
    `content_id` and `embedding_id` (when embedding succeeds inline).

    Idempotent on (source, external_id). When external_id is missing,
    synthesized from slug(source + title).
    """
    norm = _normalize_row(row, actor=actor)
    err = _validate_required(norm)
    if err:
        return {"error": err}

    if db.is_enabled():
        try:
            persisted = _upsert_pg(norm)
            if persisted:
                # Inline embedding — skip when row already has one (idempotent
                # retries of load_seed must not re-embed already-embedded rows;
                # earned 2026-05-02 when re-runs burned the gunicorn budget
                # re-embedding existing 42 rows instead of loading remaining 27).
                if not persisted.get("embedding_id"):
                    try:
                        eid = embed_one(persisted)
                        if eid:
                            persisted["embedding_id"] = eid
                    except Exception as exc:
                        logger.warning("content embed failed for %s (%s)", persisted.get("content_id"), exc)
                return persisted
        except Exception as exc:
            logger.warning("content_index upsert PG failed (%s) — using fallback", exc)

    # Fallback path
    return _upsert_fallback(norm)


def import_csv(csv_text: str, actor: str | None = None) -> dict:
    """
    Parse CSV + upsert each row. Returns summary stats + per-row errors.
    """
    if not csv_text or not csv_text.strip():
        return {"error": "empty CSV"}

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return {"error": "no header row in CSV"}

    # Normalize header names (case-insensitive, trim)
    header_map = {h.lower().strip(): h for h in reader.fieldnames if h}
    missing = [c for c in REQUIRED_COLUMNS if c not in header_map]
    if missing:
        return {"error": f"missing required columns: {', '.join(missing)}"}

    inserted = 0
    updated = 0
    embedded = 0
    errors: list[dict] = []
    rows_processed = 0

    for row_num, raw in enumerate(reader, start=2):  # start=2 because row 1 is header
        rows_processed += 1
        try:
            normalized_row: dict[str, Any] = {}
            for col in REQUIRED_COLUMNS + SOFT_COLUMNS:
                src = header_map.get(col)
                if src and src in raw:
                    normalized_row[col] = raw[src]
            existing = _find_by_natural_key(normalized_row.get("source", ""), normalized_row.get("external_id"))
            persisted = upsert(normalized_row, actor=actor)
            if persisted.get("error"):
                errors.append({"row": row_num, "msg": persisted["error"]})
                continue
            if existing:
                updated += 1
            else:
                inserted += 1
            if persisted.get("embedding_id"):
                embedded += 1
        except Exception as exc:
            errors.append({"row": row_num, "msg": f"unexpected: {exc}"})

    return {
        "rows_processed": rows_processed,
        "inserted": inserted,
        "updated": updated,
        "embedded": embedded,
        "errors": errors,
    }


def list_for_browse(filters: dict | None = None, limit: int = 100, offset: int = 0) -> dict:
    """Paginated catalog browse with filters."""
    filters = filters or {}
    limit = max(1, min(int(limit or 100), 500))
    offset = max(0, int(offset or 0))

    if db.is_enabled():
        try:
            return _list_pg(filters, limit, offset)
        except Exception as exc:
            logger.warning("content_index list PG failed (%s) — using fallback", exc)

    return _list_fallback(filters, limit, offset)


def retrieve(query_text: str, top_k: int = 30, filters: dict | None = None) -> list[dict]:
    """
    RAG retrieval — Path Engine calls this to get candidate content for
    a goal. Embeds the query, queries Pinecone, enriches results with
    full content_index row data. Falls back to keyword search when
    Pinecone is unavailable or the catalog is empty.
    """
    filters = filters or {}
    if not query_text or not query_text.strip():
        return []

    # Try vector search first
    if vector_index.is_live() or vector_index._stub_count() > 0:
        try:
            query_vec = embeddings.embed_text(query_text)
            # Only add filter keys when the caller supplied a real value.
            # Earlier we wrote `if "is_free" in filters: pinecone_filter["is_free"] = bool(filters["is_free"])`,
            # which turned a missing/None into is_free=False — excluding every
            # free-content row in the catalog and yielding zero matches.
            pinecone_filter = {}
            if filters.get("is_free") is not None:
                pinecone_filter["is_free"] = bool(filters["is_free"])
            if filters.get("source"):
                pinecone_filter["source"] = filters["source"]
            matches = vector_index.query(query_vec, top_k=top_k, filter=pinecone_filter or None)
            if matches:
                # matches is a list of {id, score, metadata}; enrich with full rows
                content_ids = [int(m.get("id", "0").replace("content-", "")) for m in matches if m.get("id")]
                rows_by_id = {r["content_id"]: r for r in _bulk_fetch(content_ids)}
                enriched = []
                for m in matches:
                    raw_id = m.get("id", "")
                    try:
                        cid = int(raw_id.replace("content-", ""))
                    except (TypeError, ValueError):
                        continue
                    row = rows_by_id.get(cid)
                    if row:
                        row = dict(row)
                        row["_score"] = float(m.get("score") or 0.0)
                        enriched.append(row)
                return enriched
        except Exception as exc:
            logger.warning("retrieve via Pinecone failed (%s) — falling back to keyword", exc)

    # Keyword fallback — substring match across title/description/skills
    return _keyword_search(query_text, top_k=top_k, filters=filters)


def delete_one(content_id: int) -> dict:
    """Remove from Pinecone first, then delete the row."""
    try:
        cid = int(content_id)
    except (TypeError, ValueError):
        return {"error": "content_id must be int"}

    # Remove from vector store
    try:
        vector_index.delete(_vector_id(cid))
    except Exception as exc:
        logger.warning("vector_index.delete failed for %s (%s) — continuing", cid, exc)

    if db.is_enabled():
        try:
            row = db.execute(
                "DELETE FROM content_index WHERE content_id = %s",
                (cid,),
            )
            return {"ok": True, "content_id": cid, "deleted": int(row or 0)}
        except Exception as exc:
            logger.warning("content_index delete PG failed (%s) — using fallback", exc)

    if cid in _FALLBACK:
        row = _FALLBACK.pop(cid)
        key = (row.get("source"), row.get("external_id"))
        _FALLBACK_BY_KEY.pop(key, None)
        return {"ok": True, "content_id": cid, "deleted": 1}
    return {"ok": True, "content_id": cid, "deleted": 0}


def embed_one(row: dict) -> str | None:
    """Compose embedding text + call Voyage + upsert Pinecone."""
    cid = row.get("content_id")
    if not cid:
        return None
    text_parts = [
        row.get("title", ""),
        row.get("description", ""),
        f"Skills: {', '.join(row.get('skills') or [])}" if row.get("skills") else "",
        f"Prerequisites: {', '.join(row.get('prerequisites') or [])}" if row.get("prerequisites") else "",
        f"Source: {row.get('source', '')}" if row.get("source") else "",
        f"Difficulty: {row.get('difficulty', '')}" if row.get("difficulty") else "",
    ]
    embed_text_compose = "\n".join(p for p in text_parts if p)
    try:
        vector = embeddings.embed_text(embed_text_compose)
    except Exception as exc:
        logger.warning("embedding failed for content %s (%s)", cid, exc)
        return None

    metadata = {
        "source": row.get("source"),
        "is_free": bool(row.get("is_free")),
        "difficulty": row.get("difficulty"),
        "content_type": row.get("content_type"),
        "title": (row.get("title") or "")[:200],  # Pinecone metadata size cap
    }
    eid = _vector_id(cid)
    try:
        vector_index.upsert(eid, vector, metadata)
    except Exception as exc:
        logger.warning("vector upsert failed for content %s (%s)", cid, exc)
        return None

    # Persist embedding_id back on the row
    if db.is_enabled():
        try:
            db.execute(
                "UPDATE content_index SET embedding_id = %s, last_synced_at = now() WHERE content_id = %s",
                (eid, cid),
            )
        except Exception as exc:
            logger.warning("content embedding_id persist failed (%s)", exc)
    if cid in _FALLBACK:
        _FALLBACK[cid]["embedding_id"] = eid
        _FALLBACK[cid]["last_synced_at"] = _now_iso()
    return eid


def embed_pending(limit: int = 200, force: bool = False) -> dict:
    """
    Backfill embeddings.

    force=False (default) → only rows where embedding_id IS NULL.
    force=True            → all rows, regardless. Used when the live
                            Pinecone index is out of sync with what the
                            DB thinks is embedded (e.g. rows were embedded
                            against the in-memory stub before Pinecone
                            env vars were set).
    """
    rows: list[dict] = []
    where_clause = "" if force else "WHERE embedding_id IS NULL"
    if db.is_enabled():
        try:
            rows = db.query(
                f"""
                SELECT content_id, source, title, source_url, content_type, duration_minutes,
                       description, skills, prerequisites, difficulty, is_free, language,
                       embedding_id
                FROM content_index
                {where_clause}
                ORDER BY content_id
                LIMIT %s
                """,
                (int(limit),),
            ) or []
        except Exception as exc:
            logger.warning("embed_pending PG read failed (%s) — using fallback", exc)
            rows = []

    if not rows:
        if force:
            rows = list(_FALLBACK.values())[:int(limit)]
        else:
            rows = [r for r in _FALLBACK.values() if not r.get("embedding_id")][:int(limit)]

    if not rows:
        return {"processed": 0, "embedded": 0, "failed": 0, "errors": []}

    # Batch embed — Voyage supports up to 128 inputs per request, so 200 rows
    # become at most 2 calls. Avoids per-row 429 rate-limiting that earlier
    # caused silent fallback to the 512-dim stub vectors. Earned 2026-05-02
    # when 69 sequential calls hit Voyage's per-minute limit.
    BATCH = 100
    errors: list[dict] = []
    embedded = 0
    failed = 0

    for batch_start in range(0, len(rows), BATCH):
        batch = rows[batch_start:batch_start + BATCH]
        texts = [_compose_embed_text(r) for r in batch]

        # One Voyage call for the whole batch — surface real errors, no silent stub fallback
        try:
            from . import embeddings as _emb
            if not _emb.is_live():
                errors.append({"batch_start": batch_start, "msg": "VOYAGE_API_KEY not set"})
                failed += len(batch)
                continue
            vectors = _emb._embed_live(texts)
        except Exception as exc:
            errors.append({"batch_start": batch_start, "msg": f"Voyage batch failed: {exc}"})
            failed += len(batch)
            continue

        if len(vectors) != len(batch):
            errors.append({"batch_start": batch_start, "msg": f"Voyage returned {len(vectors)} vectors for {len(batch)} inputs"})
            failed += len(batch)
            continue

        # Upsert each vector to Pinecone, persist embedding_id
        for r, vector in zip(batch, vectors):
            cid = r.get("content_id")
            if not cid:
                failed += 1
                continue
            metadata = {
                "source": r.get("source"),
                "is_free": bool(r.get("is_free")),
                "difficulty": r.get("difficulty"),
                "content_type": r.get("content_type"),
                "title": (r.get("title") or "")[:200],
            }
            eid = _vector_id(cid)
            try:
                vector_index.upsert(eid, vector, metadata)
            except Exception as exc:
                errors.append({"content_id": cid, "msg": f"Pinecone upsert: {exc}"})
                failed += 1
                continue

            if db.is_enabled():
                try:
                    db.execute(
                        "UPDATE content_index SET embedding_id = %s, last_synced_at = now() WHERE content_id = %s",
                        (eid, cid),
                    )
                except Exception as exc:
                    logger.warning("content embedding_id persist failed (%s)", exc)
            if cid in _FALLBACK:
                _FALLBACK[cid]["embedding_id"] = eid
                _FALLBACK[cid]["last_synced_at"] = _now_iso()
            embedded += 1

    return {"processed": len(rows), "embedded": embedded, "failed": failed, "errors": errors}


def _compose_embed_text(row: dict) -> str:
    """Same composition as embed_one — extracted so embed_pending can batch."""
    parts = [
        row.get("title", ""),
        row.get("description", ""),
        f"Skills: {', '.join(row.get('skills') or [])}" if row.get("skills") else "",
        f"Prerequisites: {', '.join(row.get('prerequisites') or [])}" if row.get("prerequisites") else "",
        f"Source: {row.get('source', '')}" if row.get("source") else "",
        f"Difficulty: {row.get('difficulty', '')}" if row.get("difficulty") else "",
    ]
    return "\n".join(p for p in parts if p)


def load_seed(seed_path: str, actor: str | None = "system-seed") -> dict:
    """Read a CSV from disk and import. Used to bootstrap the demo catalog."""
    try:
        with open(seed_path, "r", encoding="utf-8") as f:
            csv_text = f.read()
    except Exception as exc:
        return {"error": f"could not read seed file: {exc}"}
    return import_csv(csv_text, actor=actor)


def get_template_csv() -> str:
    """Return a header-only template with 5 example rows for download."""
    rows = [
        # header
        REQUIRED_COLUMNS + SOFT_COLUMNS,
        # examples
        ["NVIDIA-DLI-RAG-AGENTS", "NVIDIA DLI", "Building RAG Agents with LLMs",
         "https://learn.nvidia.com/courses/course-detail?course_id=course-v1:DLI+S-FX-15+V1",
         "course", "480",
         "Hands-on with LangChain and a vector DB; covers chunking embeddings and hybrid search.",
         "rag,langchain,vector-stores", "python", "intermediate", "true", "en"],
        ["DLAI-AGENTIC-AI", "DeepLearning.AI", "Agentic AI by Andrew Ng",
         "https://www.deeplearning.ai/short-courses/agentic-ai/",
         "course", "300",
         "The four agentic design patterns explained from first principles.",
         "agents,reflection,tool-use", "python,llm-basics", "intermediate", "true", "en"],
        ["YT-LANGCHAIN-CH", "YouTube", "LangChain Official Channel",
         "https://www.youtube.com/@LangChain",
         "video", "0",
         "Weekly framework updates and patterns. Subscribe and watch new episodes.",
         "langchain,langgraph", "", "intermediate", "true", "en"],
    ]
    buf = io.StringIO()
    w = csv.writer(buf)
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


# ──────────────────────────────────────────────────────────────
# Internals — normalization
# ──────────────────────────────────────────────────────────────

def _normalize_row(raw: dict, actor: str | None = None) -> dict:
    """Coerce CSV / API input into the canonical row shape."""
    out: dict[str, Any] = {}
    out["source"] = (raw.get("source") or "").strip()
    out["title"] = (raw.get("title") or "").strip()
    out["source_url"] = (raw.get("source_url") or "").strip()

    # Synthesize external_id when missing
    eid = (raw.get("external_id") or "").strip()
    if not eid and out["source"] and out["title"]:
        eid = _slugify(f"{out['source']}-{out['title']}")[:80]
    out["external_id"] = eid

    ct = (raw.get("content_type") or "course").strip().lower()
    out["content_type"] = ct if ct in _VALID_CONTENT_TYPES else "course"

    try:
        out["duration_minutes"] = int(raw.get("duration_minutes") or 60)
    except (TypeError, ValueError):
        out["duration_minutes"] = 60

    out["description"] = (raw.get("description") or "").strip() or None
    out["skills"] = _split_csv_list(raw.get("skills"))
    out["prerequisites"] = _split_csv_list(raw.get("prerequisites"))

    diff = (raw.get("difficulty") or "").strip().lower()
    out["difficulty"] = diff if diff in _VALID_DIFFICULTIES else None

    out["is_free"] = _coerce_bool(raw.get("is_free"), default=True)
    out["language"] = (raw.get("language") or "en").strip() or "en"

    if actor:
        out["imported_by"] = actor

    return out


def _validate_required(norm: dict) -> str | None:
    for col in REQUIRED_COLUMNS:
        v = norm.get(col)
        if v is None or (isinstance(v, str) and not v.strip()):
            return f"missing required field: {col}"
    return None


def _split_csv_list(v) -> list[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if x and str(x).strip()]
    s = str(v).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _coerce_bool(v, default: bool = True) -> bool:
    if v is None or v == "":
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("true", "yes", "y", "1", "free"):
        return True
    if s in ("false", "no", "n", "0", "paid"):
        return False
    return default


def _slugify(s: str) -> str:
    out = re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()
    return out or "untitled"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _vector_id(content_id: int) -> str:
    return f"content-{int(content_id)}"


# ──────────────────────────────────────────────────────────────
# Internals — Postgres path
# ──────────────────────────────────────────────────────────────

def _upsert_pg(row: dict) -> dict | None:
    out = db.execute_returning(
        """
        INSERT INTO content_index
            (external_id, source, title, source_url, content_type,
             duration_minutes, description, skills, prerequisites,
             difficulty, is_free, language, imported_by, contributed_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, external_id) DO UPDATE SET
            title = EXCLUDED.title,
            source_url = EXCLUDED.source_url,
            content_type = EXCLUDED.content_type,
            duration_minutes = EXCLUDED.duration_minutes,
            description = EXCLUDED.description,
            skills = EXCLUDED.skills,
            prerequisites = EXCLUDED.prerequisites,
            difficulty = EXCLUDED.difficulty,
            is_free = EXCLUDED.is_free,
            language = EXCLUDED.language,
            last_synced_at = now()
        RETURNING content_id, external_id, source, title, source_url, content_type,
                  duration_minutes, description, skills, prerequisites,
                  difficulty, is_free, language, embedding_id, quality_score,
                  indexed_at, last_synced_at, imported_by, contributed_by
        """,
        (
            row["external_id"], row["source"], row["title"], row["source_url"],
            row["content_type"], row["duration_minutes"], row.get("description"),
            row["skills"], row["prerequisites"], row.get("difficulty"),
            row["is_free"], row["language"],
            row.get("imported_by"), row.get("contributed_by"),
        ),
    )
    return _normalize_pg_row(out) if out else None


def _list_pg(filters: dict, limit: int, offset: int) -> dict:
    clauses: list[str] = []
    params: list = []
    if filters.get("source"):
        clauses.append("source = %s")
        params.append(filters["source"])
    if filters.get("difficulty"):
        clauses.append("difficulty = %s")
        params.append(filters["difficulty"])
    if filters.get("content_type"):
        clauses.append("content_type = %s")
        params.append(filters["content_type"])
    if "is_free" in filters and filters["is_free"] is not None:
        clauses.append("is_free = %s")
        params.append(bool(filters["is_free"]))
    if filters.get("needs_embedding"):
        clauses.append("embedding_id IS NULL")
    if filters.get("search"):
        s = f"%{filters['search'].lower()}%"
        clauses.append(
            "(LOWER(title) LIKE %s OR LOWER(COALESCE(description, '')) LIKE %s "
            "OR LOWER(source) LIKE %s OR EXISTS (SELECT 1 FROM unnest(skills) k WHERE LOWER(k) LIKE %s))"
        )
        params.extend([s, s, s, s])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    rows = db.query(
        f"""
        SELECT content_id, external_id, source, title, source_url, content_type,
               duration_minutes, description, skills, prerequisites,
               difficulty, is_free, language, embedding_id, quality_score,
               indexed_at, last_synced_at, imported_by, contributed_by
        FROM content_index
        {where}
        ORDER BY indexed_at DESC, content_id DESC
        LIMIT %s OFFSET %s
        """,
        params + [limit, offset],
    ) or []

    count_row = db.query_one(f"SELECT COUNT(*) AS n FROM content_index {where}", params)
    total = int(count_row["n"]) if count_row else len(rows)

    facets_rows = db.query(
        "SELECT source, COUNT(*) AS n FROM content_index GROUP BY source ORDER BY n DESC LIMIT 30"
    ) or []
    facets = {r["source"]: int(r["n"]) for r in facets_rows}

    items = [_normalize_pg_row(r) for r in rows]
    return {"items": items, "total": total, "limit": limit, "offset": offset, "facets": {"by_source": facets}}


def _bulk_fetch(content_ids: list[int]) -> list[dict]:
    if not content_ids:
        return []
    if db.is_enabled():
        try:
            rows = db.query(
                """
                SELECT content_id, external_id, source, title, source_url, content_type,
                       duration_minutes, description, skills, prerequisites,
                       difficulty, is_free, language, embedding_id
                FROM content_index
                WHERE content_id = ANY(%s)
                """,
                (list(content_ids),),
            ) or []
            return [_normalize_pg_row(r) for r in rows]
        except Exception as exc:
            logger.warning("content bulk_fetch PG failed (%s) — fallback", exc)
    return [_FALLBACK[i] for i in content_ids if i in _FALLBACK]


def _normalize_pg_row(row: dict) -> dict:
    out = dict(row)
    for k in ("indexed_at", "last_synced_at"):
        v = out.get(k)
        if v is not None and hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    out["skills"] = list(out.get("skills") or [])
    out["prerequisites"] = list(out.get("prerequisites") or [])
    return out


# ──────────────────────────────────────────────────────────────
# Internals — fallback path
# ──────────────────────────────────────────────────────────────

def _next_fallback_id() -> int:
    _FALLBACK_ID_COUNTER[0] += 1
    return _FALLBACK_ID_COUNTER[0]


def _find_by_natural_key(source: str, external_id: str | None) -> dict | None:
    if not source or not external_id:
        return None
    if db.is_enabled():
        try:
            return db.query_one(
                "SELECT content_id FROM content_index WHERE source = %s AND external_id = %s",
                (source, external_id),
            )
        except Exception:
            pass
    cid = _FALLBACK_BY_KEY.get((source, external_id))
    if cid is None:
        return None
    return _FALLBACK.get(cid)


def _upsert_fallback(norm: dict) -> dict:
    key = (norm["source"], norm["external_id"])
    existing_id = _FALLBACK_BY_KEY.get(key)
    if existing_id and existing_id in _FALLBACK:
        _FALLBACK[existing_id].update(norm)
        _FALLBACK[existing_id]["last_synced_at"] = _now_iso()
        return _FALLBACK[existing_id]

    cid = _next_fallback_id()
    row = {
        "content_id": cid,
        **norm,
        "embedding_id": None,
        "quality_score": None,
        "indexed_at": _now_iso(),
        "last_synced_at": _now_iso(),
    }
    _FALLBACK[cid] = row
    _FALLBACK_BY_KEY[key] = cid
    return row


def _list_fallback(filters: dict, limit: int, offset: int) -> dict:
    items = list(_FALLBACK.values())
    if filters.get("source"):
        items = [r for r in items if r.get("source") == filters["source"]]
    if filters.get("difficulty"):
        items = [r for r in items if r.get("difficulty") == filters["difficulty"]]
    if filters.get("content_type"):
        items = [r for r in items if r.get("content_type") == filters["content_type"]]
    if "is_free" in filters and filters["is_free"] is not None:
        items = [r for r in items if bool(r.get("is_free")) == bool(filters["is_free"])]
    if filters.get("needs_embedding"):
        items = [r for r in items if not r.get("embedding_id")]
    if filters.get("search"):
        q = filters["search"].lower()
        items = [
            r for r in items
            if q in (r.get("title") or "").lower()
            or q in (r.get("description") or "").lower()
            or q in (r.get("source") or "").lower()
            or any(q in (s or "").lower() for s in r.get("skills") or [])
        ]
    items = sorted(items, key=lambda r: -int(r.get("content_id") or 0))
    total = len(items)

    facets: dict[str, int] = {}
    for r in _FALLBACK.values():
        facets[r.get("source", "")] = facets.get(r.get("source", ""), 0) + 1

    return {
        "items": items[offset:offset + limit],
        "total": total,
        "limit": limit,
        "offset": offset,
        "facets": {"by_source": facets},
    }


def _keyword_search(query_text: str, top_k: int = 30, filters: dict | None = None) -> list[dict]:
    """Fallback retrieval when Pinecone is unavailable. Score = title-hits * 3 + skill-hits * 2 + desc-hits * 1."""
    filters = filters or {}
    res = list_for_browse(filters, limit=500)
    items = res.get("items") or []
    qtokens = [t for t in re.findall(r"[a-zA-Z0-9-]+", query_text.lower()) if len(t) > 2]
    if not qtokens:
        return items[:top_k]
    scored = []
    for r in items:
        title = (r.get("title") or "").lower()
        desc = (r.get("description") or "").lower()
        skills = " ".join(r.get("skills") or []).lower()
        score = 0.0
        for t in qtokens:
            if t in title:
                score += 3
            if t in skills:
                score += 2
            if t in desc:
                score += 1
        if score > 0:
            r2 = dict(r)
            r2["_score"] = score
            scored.append(r2)
    scored.sort(key=lambda r: r.get("_score", 0), reverse=True)
    return scored[:top_k]


def reset_for_tests():
    """Test helper — clears the in-memory fallback. Does not touch Postgres."""
    _FALLBACK.clear()
    _FALLBACK_BY_KEY.clear()
    _FALLBACK_ID_COUNTER[0] = 0
