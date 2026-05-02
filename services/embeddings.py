"""
Embeddings — V3 semantic search backbone.

Two modes, one interface:

  1. LIVE — Voyage AI (Anthropic-ecosystem partner; no new vendor
     relationship). Default model: voyage-3. Activates when VOYAGE_API_KEY
     is set.

  2. STUB — local TF-IDF-ish hashing. Maps text into a fixed-size float
     vector via token-frequency hashing. Not as good as real embeddings
     but: deterministic, dependency-free, and good enough that semantic
     queries return *plausibly* ranked results in the demo. Same
     stub-when-not-configured pattern used elsewhere.

Vector dimensions:
  - LIVE Voyage: 1024 (voyage-3) or 1536 (voyage-3-large) — matches the
    Pinecone index dim configured in vector_index.py.
  - STUB local: 512 — local-only; does NOT mix with live vectors.

INTERFACE
─────────
  embed_text(text) -> list[float]
  embed_batch(texts) -> list[list[float]]
  is_live() -> bool
  vector_dim() -> int
"""

import os
import math
import hashlib

VOYAGE_MODEL = os.environ.get("VOYAGE_MODEL", "voyage-3")
STUB_DIM = 512

# Voyage models have different output dimensions; LIVE_DIM tracks the active one.
# Used by vector_dim() and as documentation for what the Pinecone index dim
# must match. Default is whatever VOYAGE_MODEL emits.
_VOYAGE_MODEL_DIMS = {
    "voyage-3": 1024,
    "voyage-3-lite": 512,
    "voyage-3-large": 1024,   # default; supports 256/512/1024/2048 via output_dimension param (NOT 1536)
    "voyage-code-3": 1024,
    "voyage-finance-2": 1024,
    "voyage-law-2": 1024,
    "voyage-multilingual-2": 1024,
}
LIVE_DIM = _VOYAGE_MODEL_DIMS.get(VOYAGE_MODEL, 1024)


def is_live() -> bool:
    return bool(os.environ.get("VOYAGE_API_KEY"))


def vector_dim() -> int:
    return LIVE_DIM if is_live() else STUB_DIM


# ──────────────────────────────────────────────────────────────
# Voyage live mode
# ──────────────────────────────────────────────────────────────

def _embed_live(texts: list) -> list:
    import requests
    api_key = os.environ["VOYAGE_API_KEY"]
    r = requests.post(
        "https://api.voyageai.com/v1/embeddings",
        json={"input": texts, "model": VOYAGE_MODEL, "input_type": "document"},
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    return [item["embedding"] for item in sorted(data, key=lambda d: d["index"])]


# ──────────────────────────────────────────────────────────────
# Local stub — TF-IDF-ish hashing
# ──────────────────────────────────────────────────────────────

def _tokenize(text: str) -> list:
    return [t for t in text.lower().replace("\n", " ").split() if t.isalnum() and len(t) > 2]


def _embed_stub(text: str) -> list:
    vec = [0.0] * STUB_DIM
    tokens = _tokenize(text)
    if not tokens:
        return vec
    # Hash each token into 2 buckets; weight by inverse log of position
    for pos, tok in enumerate(tokens):
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        b1 = h % STUB_DIM
        b2 = (h // STUB_DIM) % STUB_DIM
        weight = 1.0 / math.log(pos + 2)
        vec[b1] += weight
        vec[b2] += weight * 0.5
    # L2 normalize so cosine == dot product
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


# ──────────────────────────────────────────────────────────────
# Public interface
# ──────────────────────────────────────────────────────────────

def embed_text(text: str) -> list:
    if is_live():
        try:
            return _embed_live([text])[0]
        except Exception as exc:
            print(f"[embeddings] Voyage failed, falling back to stub: {exc}")
    return _embed_stub(text)


def embed_batch(texts: list) -> list:
    if is_live():
        try:
            return _embed_live(texts)
        except Exception as exc:
            print(f"[embeddings] Voyage batch failed, falling back to stub: {exc}")
    return [_embed_stub(t) for t in texts]
