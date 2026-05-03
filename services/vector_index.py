"""
Vector index — V3 semantic search storage layer.

Two modes, one interface:

  1. LIVE — Pinecone serverless. Activates when PINECONE_API_KEY +
     PINECONE_INDEX are set. Index dim must match embeddings.LIVE_DIM
     (1024 for voyage-3).

  2. STUB — in-memory cosine-similarity store. Same interface, fully
     functional for the demo. Vectors live in process memory; lost on
     restart. Same stub-when-not-configured pattern.

INTERFACE
─────────
  upsert(item_id, vector, metadata) -> {ok, mode}
  query(vector, top_k, filter) -> [{id, score, metadata}, ...]
  delete(item_id) -> {ok, mode}
  count() -> int
  is_live() -> bool

The metadata payload travels alongside each vector. For content_index:
  { title, source, source_url, content_type, duration_minutes, difficulty,
    skills (list), concepts_covered (list) }
"""

import os
import math


def _has_pinecone() -> bool:
    return bool(os.environ.get("PINECONE_API_KEY")) and bool(os.environ.get("PINECONE_INDEX"))


def is_live() -> bool:
    return _has_pinecone()


# ──────────────────────────────────────────────────────────────
# In-memory stub
# ──────────────────────────────────────────────────────────────

_STORE = {}  # { id: {"vector": [...], "metadata": {...}} }


def _cosine(a: list, b: list) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(x * x for x in b)) or 1.0
    return dot / (na * nb)


def _stub_upsert(item_id: str, vector: list, metadata: dict) -> dict:
    _STORE[item_id] = {"vector": vector, "metadata": metadata or {}}
    return {"ok": True, "mode": "stub"}


def _stub_query(vector: list, top_k: int, filter: dict = None) -> list:
    candidates = []
    for item_id, entry in _STORE.items():
        if filter and not _matches_filter(entry["metadata"], filter):
            continue
        score = _cosine(vector, entry["vector"])
        candidates.append({"id": item_id, "score": round(score, 4), "metadata": entry["metadata"]})
    candidates.sort(key=lambda c: -c["score"])
    return candidates[:top_k]


def _matches_filter(metadata: dict, filt: dict) -> bool:
    """Tiny subset of Pinecone's filter syntax — exact match + $in."""
    for key, val in filt.items():
        if isinstance(val, dict) and "$in" in val:
            if metadata.get(key) not in val["$in"]:
                return False
        else:
            if metadata.get(key) != val:
                return False
    return True


def _stub_delete(item_id: str) -> dict:
    _STORE.pop(item_id, None)
    return {"ok": True, "mode": "stub"}


def _stub_count() -> int:
    return len(_STORE)


# ──────────────────────────────────────────────────────────────
# Pinecone live mode
# ──────────────────────────────────────────────────────────────

_pinecone_index = None


def _get_pinecone_index():
    global _pinecone_index
    if _pinecone_index is None and _has_pinecone():
        from pinecone import Pinecone
        pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
        _pinecone_index = pc.Index(os.environ["PINECONE_INDEX"])
    return _pinecone_index


def _live_upsert(item_id: str, vector: list, metadata: dict) -> dict:
    idx = _get_pinecone_index()
    # Pinecone metadata can't carry lists of dicts; flatten lists of strings only.
    safe_meta = {k: v for k, v in (metadata or {}).items() if isinstance(v, (str, int, float, bool, list))}
    idx.upsert(vectors=[(item_id, vector, safe_meta)])
    return {"ok": True, "mode": "live"}


def _live_query(vector: list, top_k: int, filter: dict = None) -> list:
    idx = _get_pinecone_index()
    resp = idx.query(vector=vector, top_k=top_k, filter=filter, include_metadata=True)
    return [
        {"id": m["id"], "score": round(m["score"], 4), "metadata": m.get("metadata") or {}}
        for m in (resp.get("matches") or [])
    ]


def _live_delete(item_id: str) -> dict:
    idx = _get_pinecone_index()
    idx.delete(ids=[item_id])
    return {"ok": True, "mode": "live"}


def _live_count() -> int:
    idx = _get_pinecone_index()
    stats = idx.describe_index_stats()
    return int((stats or {}).get("total_vector_count", 0))


def _live_delete_all() -> dict:
    """Pinecone supports delete_all=True per namespace. We use the default ns."""
    idx = _get_pinecone_index()
    idx.delete(delete_all=True)
    return {"ok": True, "mode": "live"}


# ──────────────────────────────────────────────────────────────
# Public interface
# ──────────────────────────────────────────────────────────────

def upsert(item_id: str, vector: list, metadata: dict = None) -> dict:
    if _has_pinecone():
        try:
            return _live_upsert(item_id, vector, metadata)
        except Exception as exc:
            print(f"[vector_index] Pinecone upsert failed, falling back to stub: {exc}")
    return _stub_upsert(item_id, vector, metadata)


def query(vector: list, top_k: int = 10, filter: dict = None) -> list:
    if _has_pinecone():
        try:
            return _live_query(vector, top_k, filter)
        except Exception as exc:
            print(f"[vector_index] Pinecone query failed, falling back to stub: {exc}")
    return _stub_query(vector, top_k, filter)


def delete_all() -> dict:
    """
    Wipe every vector from the index. Used by /admin/content/wipe_vectors
    when the user wants a full reset. Returns {ok, mode, before, after}.
    """
    before = 0
    try:
        before = count()
    except Exception:
        pass
    if _has_pinecone():
        try:
            _live_delete_all()
        except Exception as exc:
            print(f"[vector_index] Pinecone delete_all failed, also clearing stub: {exc}")
    _STORE.clear()
    after = 0
    try:
        after = count()
    except Exception:
        pass
    return {"ok": True, "mode": "live" if _has_pinecone() else "stub",
            "before": before, "after": after}


def delete(item_id: str) -> dict:
    if _has_pinecone():
        try:
            return _live_delete(item_id)
        except Exception as exc:
            print(f"[vector_index] Pinecone delete failed, falling back to stub: {exc}")
    return _stub_delete(item_id)


def count() -> int:
    if _has_pinecone():
        try:
            return _live_count()
        except Exception as exc:
            print(f"[vector_index] Pinecone count failed, falling back to stub: {exc}")
    return _stub_count()
