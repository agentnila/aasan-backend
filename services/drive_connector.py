"""
Google Drive connector — V3.

Walks a Workspace Drive (impersonating an admin or training-content owner
via DWD), lists training-tagged docs, downloads text, classifies, embeds,
and indexes.

Two modes, one interface:

  1. LIVE — same Workspace service account already used for Calendar +
     Gmail. Adds the `drive.readonly` scope. Activates when
     GOOGLE_SERVICE_ACCOUNT_KEY + GOOGLE_WORKSPACE_DOMAIN are set, plus
     either GOOGLE_DRIVE_QUERY (custom Drive search) or
     GOOGLE_DRIVE_FOLDER_ID (folder to walk). Impersonates
     GOOGLE_DRIVE_ADMIN (a Workspace user with read access to training
     content) — falls back to GMAIL_NUDGE_SENDER when unset.

  2. STUB — returns 5 deterministic demo files so /drive/index does
     something useful in the demo even before the customer hooks up
     their Workspace.

INTERFACE
─────────
  is_connected() -> bool
  list_training_files(query?, folder_id?, limit?) -> [{file_id, title, mime_type, modified_time, snippet}]
  fetch_file_text(file_id, mime_type) -> str

Higher-level orchestration (classify + embed + index) lives in app.py
under /drive/index — this module is just the Drive I/O.
"""

import os
import json
import base64

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]

DEFAULT_TRAINING_QUERY = (
    "(name contains 'Training' or name contains 'Onboarding' or name contains 'Runbook' "
    "or fullText contains 'aasan-index' or fullText contains 'training-tagged') "
    "and trashed = false and mimeType != 'application/vnd.google-apps.folder'"
)


def _has_service_account() -> bool:
    return bool(os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY")) and bool(os.environ.get("GOOGLE_WORKSPACE_DOMAIN"))


def is_connected() -> bool:
    return _has_service_account()


def _admin_email():
    return os.environ.get("GOOGLE_DRIVE_ADMIN") or os.environ.get("GMAIL_NUDGE_SENDER")


def _build_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    raw = os.environ["GOOGLE_SERVICE_ACCOUNT_KEY"]
    try:
        info = json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        info = json.loads(raw)
    subject = _admin_email()
    if not subject:
        raise RuntimeError("GOOGLE_DRIVE_ADMIN or GMAIL_NUDGE_SENDER must be set for Drive impersonation")

    creds = service_account.Credentials.from_service_account_info(
        info, scopes=DRIVE_SCOPES
    ).with_subject(subject)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


# ──────────────────────────────────────────────────────────────
# LIVE mode
# ──────────────────────────────────────────────────────────────

def _live_list(query: str = None, folder_id: str = None, limit: int = 25) -> list:
    service = _build_service()
    q = query or os.environ.get("GOOGLE_DRIVE_QUERY") or DEFAULT_TRAINING_QUERY
    if folder_id or os.environ.get("GOOGLE_DRIVE_FOLDER_ID"):
        fid = folder_id or os.environ["GOOGLE_DRIVE_FOLDER_ID"]
        q = f"'{fid}' in parents and " + q
    resp = service.files().list(
        q=q,
        pageSize=limit,
        fields="files(id, name, mimeType, modifiedTime, webViewLink, owners(emailAddress))",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = resp.get("files", []) or []
    return [
        {
            "file_id": f["id"],
            "title": f.get("name") or "(untitled)",
            "mime_type": f.get("mimeType"),
            "modified_time": f.get("modifiedTime"),
            "url": f.get("webViewLink"),
            "owner": (f.get("owners") or [{}])[0].get("emailAddress"),
        }
        for f in files
    ]


def _live_fetch_text(file_id: str, mime_type: str) -> str:
    service = _build_service()
    if mime_type == "application/vnd.google-apps.document":
        resp = service.files().export(fileId=file_id, mimeType="text/plain").execute()
        return resp.decode("utf-8") if isinstance(resp, bytes) else str(resp)
    if mime_type in ("text/plain", "text/markdown"):
        return service.files().get_media(fileId=file_id).execute().decode("utf-8", errors="ignore")
    if mime_type == "application/vnd.google-apps.spreadsheet":
        resp = service.files().export(fileId=file_id, mimeType="text/csv").execute()
        return resp.decode("utf-8", errors="ignore") if isinstance(resp, bytes) else str(resp)
    # PDFs, slides, other binaries — defer to Phase D (needs OCR / slide extractor)
    return ""


# ──────────────────────────────────────────────────────────────
# STUB mode — 5 demo files
# ──────────────────────────────────────────────────────────────

DEMO_FILES = [
    {
        "file_id": "stub-drive-1",
        "title": "Platform Team — Service Mesh Runbook",
        "mime_type": "application/vnd.google-apps.document",
        "modified_time": "2026-04-20T10:00:00Z",
        "url": "https://docs.google.com/document/d/stub-drive-1",
        "owner": "platform@example.com",
        "_text": (
            "Service Mesh Runbook. Intermediate-level walkthrough for our Istio deployment. "
            "Covers mTLS basics, traffic management with VirtualService and DestinationRule, "
            "and observability via Kiali and Jaeger. Prereqs: Kubernetes pods, deployments, services. "
            "When pods fail, check the sidecar injection. Common breakages: mTLS misconfiguration, "
            "topologySpreadConstraints (deprecated topologyKeys in 1.31)."
        ),
    },
    {
        "file_id": "stub-drive-2",
        "title": "AWS IAM Best Practices — Engineering Onboarding",
        "mime_type": "application/vnd.google-apps.document",
        "modified_time": "2026-04-15T14:30:00Z",
        "url": "https://docs.google.com/document/d/stub-drive-2",
        "owner": "secops@example.com",
        "_text": (
            "AWS IAM best practices. Beginner overview for new engineers. Roles vs users, "
            "least-privilege policies, role assumption patterns, MFA, secret rotation. "
            "Walks through cross-account assume-role and STS temporary credentials. "
            "Prereqs: AWS basics, EC2 + S3 familiarity. Strongly recommended before "
            "touching production AWS."
        ),
    },
    {
        "file_id": "stub-drive-3",
        "title": "Terraform IaC Patterns — Module Library",
        "mime_type": "application/vnd.google-apps.document",
        "modified_time": "2026-04-22T09:15:00Z",
        "url": "https://docs.google.com/document/d/stub-drive-3",
        "owner": "platform@example.com",
        "_text": (
            "Terraform IaC patterns. Intermediate-to-advanced module patterns we use across "
            "infrastructure. Module composition, remote state with S3 + DynamoDB locking, "
            "workspace strategy for env separation. Tagging conventions. CI/CD with Atlantis. "
            "Prereqs: Terraform basics, AWS IAM, VPC."
        ),
    },
    {
        "file_id": "stub-drive-4",
        "title": "Data Privacy Compliance 2026 — All Engineers",
        "mime_type": "application/vnd.google-apps.document",
        "modified_time": "2026-04-10T11:00:00Z",
        "url": "https://docs.google.com/document/d/stub-drive-4",
        "owner": "legal@example.com",
        "_text": (
            "Data Privacy Compliance 2026. Mandatory beginner overview for all engineers. "
            "PII handling, data classification (public / internal / confidential / restricted), "
            "retention policies, regional data residency. Required acknowledgment + 30-day "
            "spaced review. Replaces the 2025 version; classification labels updated."
        ),
    },
    {
        "file_id": "stub-drive-5",
        "title": "Eng Manager 1-1 Playbook",
        "mime_type": "application/vnd.google-apps.document",
        "modified_time": "2026-03-28T16:45:00Z",
        "url": "https://docs.google.com/document/d/stub-drive-5",
        "owner": "people@example.com",
        "_text": (
            "Eng Manager 1-on-1 playbook. Intermediate guide for first-time managers. "
            "Cadence (weekly 30 min), agenda templates, growth-vs-status conversation, "
            "feedback frameworks (SBI, COIN), career laddering, performance calibration. "
            "Includes a 90-day onboarding checklist for new managers."
        ),
    },
]


def _stub_list(query: str = None, folder_id: str = None, limit: int = 25) -> list:
    return [{k: v for k, v in f.items() if k != "_text"} for f in DEMO_FILES[:limit]]


def _stub_fetch_text(file_id: str, mime_type: str) -> str:
    f = next((d for d in DEMO_FILES if d["file_id"] == file_id), None)
    return f["_text"] if f else ""


# ──────────────────────────────────────────────────────────────
# Public interface
# ──────────────────────────────────────────────────────────────

def list_training_files(query: str = None, folder_id: str = None, limit: int = 25) -> list:
    if _has_service_account():
        try:
            return _live_list(query, folder_id, limit)
        except Exception as exc:
            print(f"[drive_connector] live list failed, falling back to stub: {exc}")
    return _stub_list(query, folder_id, limit)


def fetch_file_text(file_id: str, mime_type: str) -> str:
    if _has_service_account() and not file_id.startswith("stub-"):
        try:
            return _live_fetch_text(file_id, mime_type)
        except Exception as exc:
            print(f"[drive_connector] live fetch failed, falling back to stub: {exc}")
    return _stub_fetch_text(file_id, mime_type)
