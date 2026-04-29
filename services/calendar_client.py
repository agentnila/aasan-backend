"""
Google Calendar client — Project Manager Mode (V3).

Two modes, one interface:

  1. CONNECTED (Workspace service account, domain-wide delegation) — when
     GOOGLE_SERVICE_ACCOUNT_KEY (base64-encoded JSON) and
     GOOGLE_WORKSPACE_DOMAIN are set, real Calendar API calls go via
     google-api-python-client, impersonating the learner's email. This is
     the V3 stack pattern: one Workspace Admin credential, no per-user
     OAuth flow. Setup: Workspace Admin → "Domain-wide delegation" → add
     the service account's client ID + scopes
     (https://www.googleapis.com/auth/calendar).

  2. STUB — when env not set, returns a deterministic set of busy windows
     so slot computation still produces realistic output for the demo.
     Mirrors the agentService.js / sme.py "not_connected" pattern used
     elsewhere in the build.

USER IDENTITY
─────────────
`user_id` here is the learner's Workspace email (e.g. sarah@company.com).
The Clerk SSO flow already gives us this. For the demo user, anything
goes — stub mode doesn't care.

PHASE 1 STORAGE
───────────────
This module owns Google Calendar I/O only. The Schedule_Blocks store
(Table 18) lives in app.py as an in-memory list for Phase 1; Phase 2
migrates to Airtable.

INTERFACE
─────────
  list_busy_windows(user_id, window_start, window_end) -> [(start, end), ...]
  insert_event(user_id, title, start, end, description) -> {event_id, event_url}
  delete_event(user_id, event_id) -> {ok: bool}
  is_connected() -> bool
"""

import os
import json
import base64
from datetime import datetime, timedelta, timezone

CALENDAR_SCOPES = ["https://www.googleapis.com/auth/calendar"]


def _has_service_account() -> bool:
    return bool(os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY")) and bool(os.environ.get("GOOGLE_WORKSPACE_DOMAIN"))


def is_connected() -> bool:
    return _has_service_account()


def _build_service(user_email: str):
    """Construct a Calendar API client impersonating user_email via DWD."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    raw = os.environ["GOOGLE_SERVICE_ACCOUNT_KEY"]
    try:
        info = json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        info = json.loads(raw)  # also accept raw JSON in env

    creds = service_account.Credentials.from_service_account_info(
        info, scopes=CALENDAR_SCOPES
    ).with_subject(user_email)
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


# ──────────────────────────────────────────────────────────────
# STUB MODE — deterministic demo busy windows
# ──────────────────────────────────────────────────────────────

def _stub_busy_windows(window_start: datetime, window_end: datetime) -> list:
    """
    Returns a typical knowledge-worker week: standup at 09:30, design review
    Mon/Wed PM, lunch 12:30, 1:1s scattered. Anchored to window_start's date
    so the demo feels alive on any day.
    """
    busy = []
    day = window_start.date()
    end_day = window_end.date()
    while day <= end_day:
        weekday = day.weekday()  # 0=Mon
        if weekday < 5:  # weekdays only
            d = lambda h, m=0: datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=h, minute=m)
            busy.append((d(9, 30), d(10, 0)))         # daily standup
            busy.append((d(12, 30), d(13, 30)))       # lunch
            if weekday in (0, 2):                     # Mon, Wed — design review
                busy.append((d(14, 0), d(15, 30)))
            if weekday == 1:                          # Tue — 1:1 with manager
                busy.append((d(11, 0), d(11, 30)))
            if weekday == 3:                          # Thu — team sync
                busy.append((d(15, 0), d(16, 0)))
            if weekday == 4:                          # Fri — demo / retro
                busy.append((d(16, 0), d(17, 0)))
        day += timedelta(days=1)
    return [(s, e) for s, e in busy if s < window_end and e > window_start]


# ──────────────────────────────────────────────────────────────
# CONNECTED MODE — real Google Calendar API
# ──────────────────────────────────────────────────────────────

def _parse_event_time(t: dict):
    raw = t.get("dateTime") or t.get("date")
    if not raw:
        return None
    if "T" not in raw:  # all-day event — treat as full day busy
        d = datetime.fromisoformat(raw)
        return d.replace(hour=0, tzinfo=timezone.utc)
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _real_list_busy_windows(user_id, window_start, window_end):
    """
    Workspace service-account path. Uses freebusy.query for efficiency
    (single API call returns merged busy windows; respects out-of-office,
    declined events handled per Workspace policy).
    """
    service = _build_service(user_id)
    body = {
        "timeMin": window_start.isoformat(),
        "timeMax": window_end.isoformat(),
        "items": [{"id": "primary"}],
    }
    resp = service.freebusy().query(body=body).execute()
    busy = resp.get("calendars", {}).get("primary", {}).get("busy", [])
    return [
        (datetime.fromisoformat(b["start"].replace("Z", "+00:00")),
         datetime.fromisoformat(b["end"].replace("Z", "+00:00")))
        for b in busy
    ]


def _real_insert_event(user_id, title, start, end, description):
    service = _build_service(user_id)
    body = {
        "summary": title,
        "description": description or "",
        "start": {"dateTime": start.isoformat(), "timeZone": "UTC"},
        "end":   {"dateTime": end.isoformat(),   "timeZone": "UTC"},
        "reminders": {
            "useDefault": False,
            "overrides": [{"method": "popup", "minutes": 5}],
        },
        "extendedProperties": {"private": {"aasan_managed": "true"}},
    }
    event = service.events().insert(calendarId="primary", body=body).execute()
    return {
        "event_id": event["id"],
        "event_url": event.get("htmlLink") or f"https://calendar.google.com/calendar/event?eid={event['id']}",
        "mode": "live",
    }


def _real_delete_event(user_id, event_id):
    service = _build_service(user_id)
    try:
        service.events().delete(calendarId="primary", eventId=event_id).execute()
        return {"ok": True, "mode": "live"}
    except Exception as exc:
        return {"ok": False, "mode": "live", "error": str(exc)}


# ──────────────────────────────────────────────────────────────
# PUBLIC INTERFACE
# ──────────────────────────────────────────────────────────────

def list_busy_windows(user_id: str, window_start: datetime, window_end: datetime) -> list:
    if _has_service_account():
        try:
            return _real_list_busy_windows(user_id, window_start, window_end)
        except Exception as exc:
            print(f"[calendar_client] live freebusy failed, falling back to stub: {exc}")
    return _stub_busy_windows(window_start, window_end)


def insert_event(user_id: str, title: str, start: datetime, end: datetime, description: str = "") -> dict:
    if _has_service_account():
        try:
            return _real_insert_event(user_id, title, start, end, description)
        except Exception as exc:
            print(f"[calendar_client] live insert_event failed, falling back to stub: {exc}")
    stub_id = f"stub-{int(start.timestamp())}-{user_id[:8]}"
    return {
        "event_id": stub_id,
        "event_url": f"https://calendar.google.com/calendar/event?eid={stub_id}",
        "mode": "stub",
    }


def delete_event(user_id: str, event_id: str) -> dict:
    if _has_service_account() and not event_id.startswith("stub-"):
        try:
            return _real_delete_event(user_id, event_id)
        except Exception as exc:
            print(f"[calendar_client] live delete_event failed, falling back: {exc}")
    return {"ok": True, "mode": "stub"}
