"""
Goal context extractors — turn a URL / PDF / image / text-doc that a
learner attaches at goal creation into normalized text the Path Engine
can ground its prompt in.

Why this matters
────────────────
A goal name like "Become a Cloud Architect" tells Claude very little
about which Cloud Architect, at which kind of company, with which
specialties. A learner who attaches the Senior Cloud Architect job
posting at Stripe gets a path tailored to that posting's actual
required skills — multi-region, FinOps, IAM depth — instead of a
generic AWS path. Same with attaching their team's "what we do"
slide deck, or a screenshot of an internal role description.

Sources supported
─────────────────
  url       → Perplexity Computer fetch_url, returns main_text
  document  → Claude API with a "document" content block (PDFs and
              other doc types Claude supports natively); falls back to
              utf-8 decode for plain text / markdown / csv.
  image     → Claude API with an "image" content block, prompted to
              extract any role/job-relevant text content (acts as OCR
              + summarization in one call).
  text      → just decode the bytes as utf-8 (raw paste path).

Returns: (extracted_text, source_type, error) tuple. Caller decides
how to handle errors. extracted_text is bounded to ~12K chars so
downstream Claude prompts don't blow context.
"""

from __future__ import annotations

import base64
import logging
from typing import Optional

from . import claude_client, perplexity_client

logger = logging.getLogger(__name__)

MAX_EXTRACTED_CHARS = 12000  # cap so the path-gen prompt stays sane

# Mime types we recognize as plain text (decode without Claude)
_PLAINTEXT_MIME_PREFIXES = ("text/", "application/json", "application/xml")
_PLAINTEXT_EXT = (".txt", ".md", ".markdown", ".csv", ".json", ".log", ".rst")

# Mime types we send to Claude as documents (PDF + Word)
_DOCUMENT_MIMES = (
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",  # .docx
    "application/msword",  # .doc
)

# Mime types we send as images
_IMAGE_MIMES = ("image/png", "image/jpeg", "image/jpg", "image/gif", "image/webp")


def extract(
    *,
    url: Optional[str] = None,
    file_b64: Optional[str] = None,
    mime: Optional[str] = None,
    filename: Optional[str] = None,
    raw_text: Optional[str] = None,
) -> tuple[str, str, Optional[str]]:
    """
    Dispatch on whichever input was provided. Returns
    (extracted_text, source_type, error_message_or_None).

    Source-type values: 'url' | 'document' | 'image' | 'text' | 'none'.
    """
    if url and url.strip():
        return _extract_url(url.strip())
    if raw_text and raw_text.strip():
        return _truncate(raw_text.strip()), "text", None
    if file_b64:
        return _extract_file(file_b64, mime or "", filename or "")
    return "", "none", None


def _extract_url(url: str) -> tuple[str, str, Optional[str]]:
    """Fetch a URL via Perplexity Computer and return its main text."""
    try:
        result = perplexity_client.fetch_url(url, timeout_s=45)
    except Exception as exc:
        logger.warning("goal_context url fetch failed (%s)", exc)
        return "", "url", f"URL fetch error: {exc}"
    if result.get("status") != "ok":
        err = (result.get("error") or {}).get("message") or "fetch failed"
        return "", "url", f"URL fetch returned status={result.get('status')}: {err}"
    body = (result.get("result") or {})
    text = body.get("main_text") or ""
    if not text:
        return "", "url", "URL fetched but main_text was empty"
    return _truncate(text), "url", None


def _extract_file(file_b64: str, mime: str, filename: str) -> tuple[str, str, Optional[str]]:
    """Dispatch by MIME / extension to the right extractor."""
    mime = (mime or "").lower().strip()
    fname_lower = (filename or "").lower()

    # Plain text family — decode without burning a Claude call
    if any(mime.startswith(p) for p in _PLAINTEXT_MIME_PREFIXES) or fname_lower.endswith(_PLAINTEXT_EXT):
        try:
            raw = base64.b64decode(file_b64).decode("utf-8", errors="replace")
            return _truncate(raw), "text", None
        except Exception as exc:
            return "", "document", f"text decode error: {exc}"

    # PDF / Word — send to Claude as a document content block
    if mime in _DOCUMENT_MIMES or fname_lower.endswith((".pdf", ".docx", ".doc")):
        return _extract_via_claude_document(file_b64, mime or _guess_document_mime(fname_lower))

    # Image — Claude vision with OCR-style prompt
    if mime in _IMAGE_MIMES or fname_lower.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp")):
        return _extract_via_claude_image(file_b64, mime or _guess_image_mime(fname_lower))

    return "", "document", f"Unsupported file type: mime={mime!r} filename={filename!r}"


def _extract_via_claude_document(file_b64: str, mime: str) -> tuple[str, str, Optional[str]]:
    """Use Claude's document content block to read the doc + summarize."""
    if not claude_client.is_live():
        return "", "document", "Claude not configured — document extraction needs ANTHROPIC_API_KEY"

    system_prompt = (
        "You read documents (job postings, role descriptions, project briefs, syllabi, "
        "team docs) and extract the text content. Reproduce the document's substantive "
        "text faithfully. Skip headers / footers / page numbers / boilerplate. If the "
        "document is structured (sections, requirements, responsibilities), keep the "
        "structure as labeled paragraphs. Cap your output at roughly 4000 words. "
        "If the document is empty or you cannot read it, say exactly 'EMPTY DOCUMENT'."
    )
    messages = [{
        "role": "user",
        "content": [
            {
                "type": "document",
                "source": {"type": "base64", "media_type": mime, "data": file_b64},
            },
            {
                "type": "text",
                "text": "Extract the full text of this document. Preserve section structure where useful.",
            },
        ],
    }]
    try:
        text = claude_client._call_claude(
            system=system_prompt,
            messages=messages,
            max_tokens=4096,
        )
    except Exception as exc:
        logger.warning("goal_context document extract via Claude failed (%s)", exc)
        return "", "document", f"document extract error: {exc}"

    text = (text or "").strip()
    if not text or text.upper() == "EMPTY DOCUMENT":
        return "", "document", "Claude could not read the document content"
    return _truncate(text), "document", None


def _extract_via_claude_image(file_b64: str, mime: str) -> tuple[str, str, Optional[str]]:
    """Use Claude's vision to read text in an image (acts as OCR + light summarization)."""
    if not claude_client.is_live():
        return "", "image", "Claude not configured — image extraction needs ANTHROPIC_API_KEY"

    system_prompt = (
        "You read images that contain job postings, role descriptions, screenshots of "
        "LinkedIn / job boards, hand-written notes about goals, slides describing a "
        "project. Extract every readable text segment. If the image shows a structured "
        "page (headings, bullet lists, labeled fields), preserve that structure. If "
        "the image is purely visual / no useful text, describe what's shown in 1-2 "
        "sentences. If unreadable, say exactly 'UNREADABLE'."
    )
    messages = [{
        "role": "user",
        "content": [
            {
                "type": "image",
                "source": {"type": "base64", "media_type": mime, "data": file_b64},
            },
            {
                "type": "text",
                "text": "Extract the text content of this image. Preserve structure where helpful.",
            },
        ],
    }]
    try:
        text = claude_client._call_claude(
            system=system_prompt,
            messages=messages,
            max_tokens=2048,
        )
    except Exception as exc:
        logger.warning("goal_context image extract via Claude failed (%s)", exc)
        return "", "image", f"image extract error: {exc}"

    text = (text or "").strip()
    if not text or text.upper() == "UNREADABLE":
        return "", "image", "Claude could not read the image content"
    return _truncate(text), "image", None


def _truncate(s: str) -> str:
    if len(s) <= MAX_EXTRACTED_CHARS:
        return s
    return s[:MAX_EXTRACTED_CHARS - 50] + "\n\n[...content truncated...]"


def _guess_document_mime(fname_lower: str) -> str:
    if fname_lower.endswith(".pdf"):
        return "application/pdf"
    if fname_lower.endswith(".docx"):
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if fname_lower.endswith(".doc"):
        return "application/msword"
    return "application/octet-stream"


def _guess_image_mime(fname_lower: str) -> str:
    if fname_lower.endswith(".png"):
        return "image/png"
    if fname_lower.endswith((".jpg", ".jpeg")):
        return "image/jpeg"
    if fname_lower.endswith(".gif"):
        return "image/gif"
    if fname_lower.endswith(".webp"):
        return "image/webp"
    return "image/png"
