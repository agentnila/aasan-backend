"""
Notifications dispatcher — Project Manager Mode V3 / Phase C.

Two channels, one interface:

  1. GMAIL — via the same Workspace service account + domain-wide delegation.
     Adds gmail.send scope to the existing DWD setup; no new vendor. Sender
     impersonates a configured address (env: GMAIL_NUDGE_SENDER, e.g.
     "noreply@<workspace-domain>"). Recipient is the learner's primary
     email (Workspace SSO-provided).

  2. SLACK — via incoming webhook URL (env: SLACK_WEBHOOK_URL). Optional;
     when set, every nudge also pings a configured channel. Useful for
     team-visible learning nudges or as a backup for personal channels.

Both stub-when-not-configured: if env vars are missing, dispatch logs the
intent and reports `mode: "stub"`. Phase D additions: Google Chat (same
service account, chat.spaces.send scope) + web push (VAPID + service worker).

INTERFACE
─────────
  is_configured() -> {gmail: bool, slack: bool}
  dispatch_nudge(nudge, channels=None) -> {channel: {ok, mode, error?}}

`nudge` shape comes straight from NUDGE_LOG rows in app.py:
  { block_id, employee_id, step_title, start_at, dispatched_at, channel }
"""

import os
import json
import base64
from datetime import datetime
from email.mime.text import MIMEText

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def _has_gmail() -> bool:
    return (
        bool(os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY"))
        and bool(os.environ.get("GMAIL_NUDGE_SENDER"))
    )


def _has_slack() -> bool:
    return bool(os.environ.get("SLACK_WEBHOOK_URL"))


def is_configured() -> dict:
    return {"gmail": _has_gmail(), "slack": _has_slack()}


# ──────────────────────────────────────────────────────────────
# Gmail — same service account as Calendar, different scope
# ──────────────────────────────────────────────────────────────

def _send_gmail(to_email: str, subject: str, body_text: str) -> dict:
    if not _has_gmail():
        return {"ok": False, "mode": "stub", "error": "GOOGLE_SERVICE_ACCOUNT_KEY or GMAIL_NUDGE_SENDER unset"}

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        raw = os.environ["GOOGLE_SERVICE_ACCOUNT_KEY"]
        try:
            info = json.loads(base64.b64decode(raw).decode("utf-8"))
        except Exception:
            info = json.loads(raw)

        sender = os.environ["GMAIL_NUDGE_SENDER"]
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=GMAIL_SCOPES
        ).with_subject(sender)
        service = build("gmail", "v1", credentials=creds, cache_discovery=False)

        msg = MIMEText(body_text)
        msg["to"] = to_email
        msg["from"] = sender
        msg["subject"] = subject
        encoded = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        sent = service.users().messages().send(userId="me", body={"raw": encoded}).execute()
        return {"ok": True, "mode": "live", "message_id": sent.get("id")}
    except Exception as exc:
        return {"ok": False, "mode": "live", "error": str(exc)}


# ──────────────────────────────────────────────────────────────
# Slack — incoming webhook
# ──────────────────────────────────────────────────────────────

def _send_slack(text: str) -> dict:
    if not _has_slack():
        return {"ok": False, "mode": "stub", "error": "SLACK_WEBHOOK_URL unset"}
    import requests
    try:
        r = requests.post(
            os.environ["SLACK_WEBHOOK_URL"],
            json={"text": text},
            timeout=10,
        )
        r.raise_for_status()
        return {"ok": True, "mode": "live"}
    except Exception as exc:
        return {"ok": False, "mode": "live", "error": str(exc)}


# ──────────────────────────────────────────────────────────────
# Public — orchestrator
# ──────────────────────────────────────────────────────────────

def dispatch_nudge(nudge: dict, channels: list = None) -> dict:
    """
    Send a 5-min-prior nudge via the requested channels. When `channels` is
    None, attempts every configured channel. Returns per-channel result.
    Never raises — failures land in the result dict.
    """
    channels = channels or ["gmail", "slack"]
    results = {}

    start_at = nudge.get("start_at", "")
    step_title = nudge.get("step_title", "Learning session")
    pretty_time = _pretty_time(start_at)
    subject = f"📚 In 5 minutes: {step_title}"
    body = (
        f"Your Aasan learning block starts at {pretty_time}.\n\n"
        f"Topic: {step_title}\n\n"
        f"Open Aasan to start: https://aasan-v2.vercel.app\n\n"
        f"— Peraasan"
    )

    if "gmail" in channels:
        to = nudge.get("employee_id", "")
        if "@" in to:
            results["gmail"] = _send_gmail(to, subject, body)
        else:
            results["gmail"] = {"ok": False, "mode": "stub", "error": "employee_id is not an email"}

    if "slack" in channels:
        slack_text = f"*{subject}*\n{body}"
        results["slack"] = _send_slack(slack_text)

    return results


def _pretty_time(iso: str) -> str:
    if not iso:
        return "soon"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%a %b %-d, %-I:%M %p UTC")
    except Exception:
        return iso
