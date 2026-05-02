"""
Resume Module — living service record + job-tailored resume generator.

THE TWO PROBLEMS THIS SOLVES
────────────────────────────
1. "I forget what I actually did three months ago when I need to write a resume."
2. "I have to rewrite my resume for every job — tedious, often skipped."

THE LOOP
────────
  Daily/weekly: learner tells Peraasan what they did
    → Peraasan extracts structured journal entries
    → stored as a permanent service record

  When learner shares a job posting URL:
    → Perplexity Computer reads the posting deeply
    → Claude matches journal entries to job requirements
    → Returns a tailored resume with matching projects, quantified outcomes,
       transferable skills, and "what's missing" gaps

WHY IT FITS CAREER COMPASS
──────────────────────────
Career Compass already has:
  - Market Watch (what's required)
  - Stay Ahead (where you could go + AI resilience)
  - Scenario Simulator (project paths)

Resume Module is the BRIDGE between "where you've been" (your record) and
"where you're going" (your next job). Without it, the rest of Career Compass
gives advice the learner can't actually act on (you can't apply without a
resume, and a generic resume loses to a tailored one).

STORAGE
───────
Dual-mode: persists to Supabase Postgres table `journal_entries` when
SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY env vars are set. Falls back to an
in-memory dict keyed by user_id when env vars are absent (local dev, demo
mode). Schema in `migrations/0001_init.sql` (Table 26 in V2 Data Model).
The in-memory `_FEED` (peer notification queue) stays in-process for now —
it's transient and not part of Tier 0 persistence scope.
"""

import json
import logging
from datetime import datetime
from . import perplexity_client, claude_client, db

logger = logging.getLogger(__name__)


# In-memory fallback store: { user_id: [entry, entry, ...] }
# When db.is_enabled(), the Postgres `journal_entries` table is the source of truth;
# this dict is used for local dev / Postgres-down fallback only.
_JOURNAL = {}
_DEMO_SEEDED_USERS_PG: set[str] = set()  # tracks which users have been demo-seeded into Postgres


# ──────────────────────────────────────────────────────────────
# Demo seed — Sarah Chen's service record (varied entries)
# ──────────────────────────────────────────────────────────────

DEMO_ENTRIES = [
    {
        "entry_id": "j-001",
        "date": "2026-04-15",
        "title": "Shipped multi-region failover for primary API",
        "category": "project",
        "description": "Designed and shipped automated multi-region failover for the primary customer-facing API. Coordinated across SRE, platform, and DBA teams.",
        "outcomes": ["Reduced RTO from 30 min → 4 min", "Zero downtime during rollout", "Pattern reused by 3 other services"],
        "technologies": ["AWS Route 53", "Aurora Global", "Terraform", "Kubernetes"],
        "stakeholders": ["VP Eng (sponsor)", "SRE team (3 collaborators)", "DBA team (2 collaborators)"],
        "transferable_skills": ["Cross-team coordination", "System design", "Production operations"],
    },
    {
        "entry_id": "j-002",
        "date": "2026-03-28",
        "title": "Onboarded 3 new engineers to platform team",
        "category": "mentoring",
        "description": "Designed and led the platform onboarding program for Q1 hires. Wrote the onboarding doc, paired daily for first 2 weeks, ran weekly office hours for next month.",
        "outcomes": ["3/3 hires shipped first PR within week 1", "Onboarding time-to-productive dropped from 8 weeks to 4 weeks", "Doc adopted as team standard"],
        "technologies": ["Python", "Kubernetes", "Internal tools"],
        "stakeholders": ["Engineering Manager", "3 new hires", "Platform team"],
        "transferable_skills": ["Mentoring", "Technical communication", "Documentation"],
    },
    {
        "entry_id": "j-003",
        "date": "2026-03-12",
        "title": "Led incident response for the Stripe API outage",
        "category": "crisis_response",
        "description": "Incident commander for 3-hour Stripe API outage affecting checkout. Coordinated 8 responders across 3 teams. Wrote post-mortem.",
        "outcomes": ["Restored service in 47 min (SLA: 60 min)", "Identified root cause: rate limiter misconfiguration", "Post-mortem actions adopted org-wide"],
        "technologies": ["PagerDuty", "Datadog", "AWS"],
        "stakeholders": ["VP Eng", "8 responders", "Customer Success (downstream comms)"],
        "transferable_skills": ["Crisis leadership", "Cross-functional coordination", "Root cause analysis", "Written communication"],
    },
    {
        "entry_id": "j-004",
        "date": "2026-02-20",
        "title": "Architecture review with VP Eng — got buy-in for K8s migration",
        "category": "presentation",
        "description": "Presented the case for migrating off ECS to Kubernetes to VP Eng + 4 directors. Built the proposal, ran the meeting, handled objections.",
        "outcomes": ["Got approval for $400K migration project", "Now on the migration team", "Established credibility with leadership"],
        "technologies": ["Kubernetes", "ECS", "AWS"],
        "stakeholders": ["VP Eng", "4 Engineering Directors", "Platform Eng team"],
        "transferable_skills": ["Executive communication", "Technical writing", "Stakeholder management"],
    },
    {
        "entry_id": "j-005",
        "date": "2026-02-08",
        "title": "Built internal cost-reporting tool",
        "category": "project",
        "description": "Built a Slackbot that reports per-team AWS cost daily and surfaces unusual spikes. Solo project in personal time + 2 weekends.",
        "outcomes": ["Saved ~$12K/month within 3 months of launch", "Adopted by 6 teams", "Caught a runaway Lambda billing spike that would have cost $40K"],
        "technologies": ["Python", "AWS Cost Explorer API", "Slack API", "Lambda"],
        "stakeholders": ["Finance team (collaborator)", "6 engineering teams"],
        "transferable_skills": ["Initiative", "FinOps", "End-to-end ownership"],
    },
    {
        "entry_id": "j-006",
        "date": "2026-01-22",
        "title": "Customer escalation: Acme Corp performance issue",
        "category": "customer",
        "description": "Acme Corp (top-10 customer) experienced 4x latency spike. Worked directly with their CTO over 2 days to diagnose and fix.",
        "outcomes": ["Identified upstream API misconfiguration", "Got latency back to baseline within 36 hours", "Acme renewed contract"],
        "technologies": ["Datadog", "AWS", "Postman"],
        "stakeholders": ["Acme CTO", "Customer Success", "Account team"],
        "transferable_skills": ["Customer-facing", "Diagnostic problem-solving", "Calm under pressure"],
    },
    {
        "entry_id": "j-007",
        "date": "2026-01-08",
        "title": "Added Service Mesh (Istio) to platform",
        "category": "tech_adoption",
        "description": "Led the introduction of Istio to the platform stack. Designed the rollout, wrote the migration guide, onboarded 4 services in pilot.",
        "outcomes": ["4 pilot services migrated with zero incidents", "Established mTLS between services", "Pattern documented for org-wide rollout"],
        "technologies": ["Istio", "Kubernetes", "Helm"],
        "stakeholders": ["Platform team", "4 service owner teams"],
        "transferable_skills": ["Tech adoption planning", "Migration management", "Documentation"],
    },
    {
        "entry_id": "j-008",
        "date": "2025-12-15",
        "title": "Wrote on-call escalation runbook",
        "category": "documentation",
        "description": "Wrote the canonical on-call escalation runbook for the platform team. Covers Sev-1/2/3 procedures, contact rotation, decision trees.",
        "outcomes": ["Used by team of 8 every on-call shift", "Cut Sev-1 escalation time from 12 min → 4 min average", "Adopted as template for 2 other teams"],
        "technologies": ["Confluence", "PagerDuty"],
        "stakeholders": ["Platform team (8)", "SRE team", "Engineering Manager"],
        "transferable_skills": ["Process design", "Documentation", "Operational thinking"],
    },
]


def _ensure_user(user_id: str) -> list[dict]:
    """
    Return the journal list for a user. When Postgres is enabled, reads fresh
    from the `journal_entries` table on every call so the social mutations
    (endorsements / share) made in earlier requests are visible. When disabled,
    uses _JOURNAL as an in-process cache. demo-user gets the DEMO_ENTRIES seed
    on first access either way.
    """
    if db.is_enabled():
        if user_id == "demo-user" and user_id not in _DEMO_SEEDED_USERS_PG:
            _maybe_seed_demo_user_pg()
            _DEMO_SEEDED_USERS_PG.add(user_id)
        try:
            entries = _load_user_journal_pg(user_id)
            _JOURNAL[user_id] = entries  # buffer for in-request mutations
            return entries
        except Exception as exc:
            logger.warning("journal PG load failed (%s) — falling back to in-memory", exc)

    if user_id not in _JOURNAL:
        if user_id == "demo-user":
            _JOURNAL[user_id] = list(DEMO_ENTRIES)
        else:
            _JOURNAL[user_id] = []
    return _JOURNAL[user_id]


def _maybe_seed_demo_user_pg():
    """If demo-user has no journal entries in Postgres, write the demo seed."""
    try:
        existing = db.query_one(
            "SELECT COUNT(*) AS n FROM journal_entries WHERE user_id = %s",
            ("demo-user",),
        )
        if existing and int(existing.get("n", 0)) > 0:
            return
        for entry in DEMO_ENTRIES:
            _upsert_entry_pg("demo-user", entry)
        logger.info("Seeded %d demo journal entries for demo-user", len(DEMO_ENTRIES))
    except Exception as exc:
        logger.warning("demo-user journal seed failed (%s)", exc)


def _load_user_journal_pg(user_id: str) -> list[dict]:
    """Read all journal entries for a user from Postgres, newest first by date."""
    rows = db.query(
        """
        SELECT entry_id, entry_external_id, entry_date, title, category,
               description, outcomes, technologies, stakeholders,
               transferable_skills, raw_input, company, project, author_id,
               endorsements, shared_with, captured_at, updated_at
        FROM journal_entries
        WHERE user_id = %s
        ORDER BY entry_date DESC, captured_at DESC
        """,
        (user_id,),
    ) or []
    return [_row_to_entry(r) for r in rows]


def _row_to_entry(row: dict) -> dict:
    """Normalize a journal_entries row to the entry dict shape used by callers."""
    return {
        "entry_id": row.get("entry_external_id") or f"j-{row.get('entry_id')}",
        "date": row["entry_date"].isoformat() if hasattr(row.get("entry_date"), "isoformat") else row.get("entry_date"),
        "title": row.get("title", ""),
        "category": row.get("category", "project"),
        "description": row.get("description", ""),
        "outcomes": _coerce_json_list(row.get("outcomes")),
        "technologies": list(row.get("technologies") or []),
        "stakeholders": _coerce_json_list(row.get("stakeholders")),
        "transferable_skills": list(row.get("transferable_skills") or []),
        "raw_input": row.get("raw_input"),
        "company": row.get("company") or "",
        "project": row.get("project") or "",
        "author_id": row.get("author_id"),
        "endorsements": _coerce_json_list(row.get("endorsements")),
        "shared_with": _coerce_json_list(row.get("shared_with")),
        "captured_at": row["captured_at"].isoformat() if hasattr(row.get("captured_at"), "isoformat") else row.get("captured_at"),
    }


def _coerce_json_list(v) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, list) else []
        except (ValueError, TypeError):
            return []
    return list(v) if hasattr(v, "__iter__") else []


def _upsert_entry_pg(user_id: str, entry: dict) -> dict | None:
    """Insert-or-update a single journal entry. Returns the persisted row."""
    if not db.is_enabled():
        return None
    try:
        row = db.execute_returning(
            """
            INSERT INTO journal_entries
                (user_id, entry_external_id, entry_date, title, category,
                 description, outcomes, technologies, stakeholders,
                 transferable_skills, raw_input, company, project, author_id,
                 endorsements, shared_with, captured_at)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
            ON CONFLICT (user_id, entry_external_id) DO UPDATE SET
                entry_date = EXCLUDED.entry_date,
                title = EXCLUDED.title,
                category = EXCLUDED.category,
                description = EXCLUDED.description,
                outcomes = EXCLUDED.outcomes,
                technologies = EXCLUDED.technologies,
                stakeholders = EXCLUDED.stakeholders,
                transferable_skills = EXCLUDED.transferable_skills,
                raw_input = EXCLUDED.raw_input,
                company = EXCLUDED.company,
                project = EXCLUDED.project,
                author_id = EXCLUDED.author_id,
                endorsements = EXCLUDED.endorsements,
                shared_with = EXCLUDED.shared_with
            RETURNING entry_id, entry_external_id, entry_date, title, category,
                      description, outcomes, technologies, stakeholders,
                      transferable_skills, raw_input, company, project, author_id,
                      endorsements, shared_with, captured_at, updated_at
            """,
            (
                user_id,
                entry.get("entry_id"),  # legacy id → entry_external_id
                entry.get("date") or datetime.utcnow().date().isoformat(),
                entry.get("title", ""),
                entry.get("category", "project"),
                entry.get("description", ""),
                json.dumps(list(entry.get("outcomes") or [])),
                list(entry.get("technologies") or []),
                json.dumps(list(entry.get("stakeholders") or [])),
                list(entry.get("transferable_skills") or []),
                entry.get("raw_input"),
                entry.get("company") or "",
                entry.get("project") or "",
                entry.get("author_id") or user_id,
                json.dumps(list(entry.get("endorsements") or [])),
                json.dumps(list(entry.get("shared_with") or [])),
                entry.get("captured_at") or datetime.utcnow().isoformat(),
            ),
        )
        return row
    except Exception as exc:
        logger.warning("journal upsert failed for %s/%s (%s)", user_id, entry.get("entry_id"), exc)
        return None


def _persist_entry(user_id: str, entry: dict) -> None:
    """
    Persist a single entry's current state to Postgres. Called after every
    social mutation (endorsement add/decline, share). No-op when fallback.
    """
    if db.is_enabled():
        _upsert_entry_pg(user_id, entry)


# ──────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────

def add_entry(user_id: str, raw_input: str = "", structured: dict = None) -> dict:
    """
    Capture a journal entry. Two paths:
      - Conversational: raw_input from chat → Claude extracts structured fields
      - Direct: caller provides structured dict (UI form, etc.)
    """
    journal = _ensure_user(user_id)

    # Pull social/context fields out of structured before merging — these
    # have a separate shape (lists of objects) we want to handle explicitly.
    company = (structured or {}).get("company") if structured else None
    project = (structured or {}).get("project") if structured else None
    peers_share = (structured or {}).get("peers_to_share_with") or []
    peers_endorse = (structured or {}).get("peers_to_endorse") or []

    if structured:
        entry = {
            "entry_id": f"j-{int(datetime.utcnow().timestamp())}",
            "date": structured.get("date") or datetime.utcnow().date().isoformat(),
            "captured_at": datetime.utcnow().isoformat(),
            "raw_input": raw_input,
            **structured,
        }
    else:
        # Extract from conversational input via Claude (or stub)
        extracted = _extract_entry_from_text(raw_input)
        entry = {
            "entry_id": f"j-{int(datetime.utcnow().timestamp())}",
            "date": datetime.utcnow().date().isoformat(),
            "captured_at": datetime.utcnow().isoformat(),
            "raw_input": raw_input,
            **extracted,
        }

    # Always-present social fields
    entry["company"] = (company or "").strip()
    entry["project"] = (project or "").strip()
    entry["author_id"] = user_id
    entry["endorsements"] = []
    entry["shared_with"] = []

    # Apply share + endorsement requests, generating feed events as side effects
    # (these mutate `entry["shared_with"]` / `entry["endorsements"]` in place)
    if peers_share:
        share_entry(user_id, entry["entry_id"], peers_share, _entry_ref=entry, _suppress_journal_lookup=True)
    if peers_endorse:
        request_endorsements(user_id, entry["entry_id"], peers_endorse, _entry_ref=entry, _suppress_journal_lookup=True)

    journal.append(entry)
    _persist_entry(user_id, entry)
    return {
        "entry": entry,
        "journal_size": len(journal),
        "modes": {"classifier": "live" if claude_client.is_live() else "stub"},
    }


# ──────────────────────────────────────────────────────────────
# Social layer — endorsements, share, peer feed
# Phase 1: in-memory ENDORSEMENTS + FEED. Phase 2: Airtable Tables 26b/26c.
#
# Identity model: peers identified by email. When the peer signs in to
# Aasan with the same email, their feed includes any pending requests.
# Until then, requests sit dormant (and can be expedited via email — Phase D).
# ──────────────────────────────────────────────────────────────

# { peer_email: [feed_event, ...] }   — newest at end
_FEED = {}


def _emit_feed(peer_email: str, event: dict) -> None:
    if not peer_email:
        return
    key = peer_email.lower().strip()
    if not key:
        return
    _FEED.setdefault(key, []).append({**event, "feed_id": f"f-{int(datetime.utcnow().timestamp() * 1000)}-{len(_FEED.get(key, []))}", "created_at": datetime.utcnow().isoformat()})


def _find_entry(user_id: str, entry_id: str):
    journal = _ensure_user(user_id)
    return next((e for e in journal if e.get("entry_id") == entry_id), None)


def share_entry(user_id: str, entry_id: str, peer_emails: list, _entry_ref=None, _suppress_journal_lookup=False) -> dict:
    """
    Share an existing journal entry with one or more peer emails.
    Each peer gets a `shared_entry` event in their feed.
    """
    entry = _entry_ref if _suppress_journal_lookup else _find_entry(user_id, entry_id)
    if not entry:
        return {"error": f"entry {entry_id} not found"}

    entry.setdefault("shared_with", [])
    entry.setdefault("endorsements", [])
    cleaned = [e.strip().lower() for e in (peer_emails or []) if e and e.strip()]
    for email in cleaned:
        if email in entry["shared_with"]:
            continue
        entry["shared_with"].append(email)
        _emit_feed(email, {
            "type": "shared_entry",
            "from_user_id": user_id,
            "entry_id": entry["entry_id"],
            "entry_title": entry.get("title"),
            "entry_date": entry.get("date"),
            "entry_company": entry.get("company"),
            "entry_project": entry.get("project"),
            "entry_outcomes": (entry.get("outcomes") or [])[:2],
        })
    # Persist the new shared_with state (no-op when fallback or when called
    # via add_entry, which persists once at the end).
    if not _suppress_journal_lookup:
        _persist_entry(user_id, entry)
    return {"ok": True, "shared_with": entry["shared_with"], "count": len(entry["shared_with"])}


def request_endorsements(user_id: str, entry_id: str, peer_emails: list, _entry_ref=None, _suppress_journal_lookup=False) -> dict:
    """
    Ask peers to endorse the entry. Each peer gets an `endorsement_requested`
    event. The entry tracks pending endorsements; when the peer endorses,
    status flips to approved.
    """
    entry = _entry_ref if _suppress_journal_lookup else _find_entry(user_id, entry_id)
    if not entry:
        return {"error": f"entry {entry_id} not found"}

    cleaned = [e.strip().lower() for e in (peer_emails or []) if e and e.strip()]
    for email in cleaned:
        existing = next((en for en in entry["endorsements"] if en.get("endorser_email") == email), None)
        if existing:
            continue
        entry["endorsements"].append({
            "endorser_email": email,
            "endorser_name": None,
            "endorser_role": None,
            "status": "pending",
            "requested_at": datetime.utcnow().isoformat(),
            "endorsed_at": None,
            "comment": "",
        })
        _emit_feed(email, {
            "type": "endorsement_requested",
            "from_user_id": user_id,
            "entry_id": entry["entry_id"],
            "entry_title": entry.get("title"),
            "entry_date": entry.get("date"),
            "entry_company": entry.get("company"),
            "entry_project": entry.get("project"),
            "entry_outcomes": (entry.get("outcomes") or [])[:2],
        })
    if not _suppress_journal_lookup:
        _persist_entry(user_id, entry)
    return {"ok": True, "endorsements": entry["endorsements"]}


def decline_endorsement(author_user_id: str, entry_id: str, endorser_email: str, reason: str = "") -> dict:
    """
    Peer declines an endorsement request. Flips status to 'declined' and
    emits a feed event for the author so they know not to wait.
    """
    entry = _find_entry(author_user_id, entry_id)
    if not entry:
        return {"error": f"entry {entry_id} not found"}

    email = (endorser_email or "").strip().lower()
    existing = next((e for e in entry["endorsements"] if e.get("endorser_email") == email), None)
    if existing:
        existing.update({
            "status": "declined",
            "endorsed_at": datetime.utcnow().isoformat(),
            "comment": reason or existing.get("comment", ""),
        })
    else:
        return {"error": "no pending endorsement request for this email"}

    _emit_feed(_user_email_hint(author_user_id), {
        "type": "endorsement_declined",
        "from_user_email": email,
        "entry_id": entry["entry_id"],
        "entry_title": entry.get("title"),
        "reason": reason,
    })
    _persist_entry(author_user_id, entry)
    return {"ok": True, "entry_id": entry_id, "endorsement": existing}


def endorse_entry(author_user_id: str, entry_id: str, endorser_email: str,
                  endorser_name: str = "", endorser_role: str = "", comment: str = "") -> dict:
    """
    Peer adds their endorsement to an entry (typically via the feed CTA).
    Flips the matching endorsement record to status='approved' and emits a
    `endorsement_received` feed event for the author.
    """
    entry = _find_entry(author_user_id, entry_id)
    if not entry:
        return {"error": f"entry {entry_id} not found"}

    email = (endorser_email or "").strip().lower()
    if not email:
        return {"error": "endorser_email required"}

    existing = next((e for e in entry["endorsements"] if e.get("endorser_email") == email), None)
    if existing:
        existing.update({
            "endorser_name": endorser_name or existing.get("endorser_name") or email,
            "endorser_role": endorser_role or existing.get("endorser_role") or "",
            "status": "approved",
            "endorsed_at": datetime.utcnow().isoformat(),
            "comment": comment or existing.get("comment", ""),
        })
    else:
        entry["endorsements"].append({
            "endorser_email": email,
            "endorser_name": endorser_name or email,
            "endorser_role": endorser_role,
            "status": "approved",
            "requested_at": None,
            "endorsed_at": datetime.utcnow().isoformat(),
            "comment": comment,
        })

    # Author's feed gets the receipt
    _emit_feed(_user_email_hint(author_user_id), {
        "type": "endorsement_received",
        "from_user_email": email,
        "from_user_name": endorser_name or email,
        "from_user_role": endorser_role,
        "entry_id": entry["entry_id"],
        "entry_title": entry.get("title"),
        "comment": comment,
    })
    _persist_entry(author_user_id, entry)
    return {"ok": True, "entry_id": entry_id, "endorsement": existing or entry["endorsements"][-1]}


def get_feed(user_email: str, limit: int = 25) -> dict:
    """
    Return the activity feed for a user. Looks up by email.
    Newest first. Includes pending endorsement requests as actionable items.
    """
    if not user_email:
        return {"events": [], "count": 0}
    events = list(reversed(_FEED.get(user_email.lower().strip(), [])))[:limit]
    return {
        "user_email": user_email,
        "events": events,
        "count": len(events),
    }


def _user_email_hint(user_id: str) -> str:
    """Best-effort: most user_id values in V3 ARE Workspace emails."""
    return user_id if "@" in (user_id or "") else ""


def list_journal(user_id: str, limit: int = 50) -> dict:
    """Return all journal entries for a user (most recent first)."""
    journal = _ensure_user(user_id)
    sorted_entries = sorted(journal, key=lambda e: e.get("date", ""), reverse=True)
    return {
        "user_id": user_id,
        "entry_count": len(journal),
        "entries": sorted_entries[:limit],
        "by_category": _count_by_category(journal),
    }


def tailor_resume(user_id: str, job_url: str = "", job_description: str = "") -> dict:
    """
    The killer feature. Given a job posting (URL or pasted text), return a
    tailored resume drawing from the user's journal entries.

    Pipeline:
      1. Read job posting (Perplexity Computer if URL, direct if text)
      2. Match journal entries to job requirements (Claude reasoning)
      3. Build tailored resume sections
      4. Identify gaps the user should know about
    """
    journal = _ensure_user(user_id)

    # 1. Read the job posting
    job_data = _fetch_job_posting(job_url, job_description)

    # 2. Match journal entries against the job (stub: keyword + category matching)
    matches = _match_entries(journal, job_data)

    # 3. Build the tailored resume
    if claude_client.is_live() and not job_data.get("_stub"):
        return _live_tailor(user_id, job_data, journal, matches)
    return _stub_tailor(user_id, job_data, journal, matches)


# ──────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────

def _count_by_category(entries):
    counts = {}
    for e in entries:
        cat = e.get("category", "uncategorized")
        counts[cat] = counts.get(cat, 0) + 1
    return counts


def _fetch_job_posting(url: str, text: str) -> dict:
    """
    Resolve job posting content from URL and/or pasted text.

    Resolution order:
      1. If pasted `text` is provided, use it as the source of truth — even
         when a URL is also given. (Skips the Perplexity dependency for
         demos and lets users override a flaky scrape.)
      2. Otherwise, fetch the URL via Perplexity Computer.
      3. If Perplexity errors AND no text was provided, return a stub
         marker so the caller falls back to _stub_tailor.

    `_stub: False` signals "we have real JD content; live tailor can run."
    `_stub: True` signals "fall back to canned tailor."
    """
    text = (text or "").strip()
    url = (url or "").strip()

    if text:
        return {
            "url": url or None,
            "title": "Pasted job description",
            "company": _guess_company_from_url(url) if url else "Unknown",
            "raw_text": text[:8000],
            "_stub": False,
        }

    if not url:
        return {"_stub": True, "title": "No job posting", "raw_text": "", "url": None, "company": "—"}

    fetch = perplexity_client.fetch_url(url)
    if fetch.get("status") != "ok":
        return {"_stub": True, "url": url, "title": "Could not fetch", "raw_text": "", "company": "—"}

    fetched = fetch.get("result", {})
    return {
        "url": url,
        "title": fetched.get("title", "Job posting"),
        "company": _guess_company_from_url(url),
        "raw_text": fetched.get("main_text", ""),
        "_stub": fetch.get("metadata", {}).get("mode") == "stub",
    }


def _guess_company_from_url(url):
    """Cheap company name guess from URL — Phase 2 uses Claude for accuracy."""
    if not url:
        return "—"
    domain = url.replace("https://", "").replace("http://", "").split("/")[0].lower()
    for known in ["stripe", "snowflake", "datadog", "anthropic", "vercel", "hashicorp", "aws", "amazon", "google", "microsoft", "meta", "linkedin", "indeed"]:
        if known in domain:
            return known.capitalize()
    return domain.split(".")[0].capitalize() if domain else "Unknown"


def _match_entries(journal: list, job_data: dict) -> list:
    """
    Score each journal entry against the job posting.
    Phase 1: keyword overlap on title + technologies + transferable_skills.
    Phase 2: Claude semantic matching.
    """
    job_text = (job_data.get("title", "") + " " + job_data.get("raw_text", "")).lower()
    if not job_text.strip():
        return []

    scored = []
    for entry in journal:
        score = 0.0
        # Tech overlap
        for tech in entry.get("technologies", []):
            if tech.lower() in job_text:
                score += 0.15
        # Transferable skill overlap
        for skill in entry.get("transferable_skills", []):
            if skill.lower() in job_text:
                score += 0.1
        # Category match (specific kinds of work job mentions)
        if entry.get("category") == "crisis_response" and any(k in job_text for k in ["incident", "on-call", "sre", "reliability"]):
            score += 0.2
        if entry.get("category") == "mentoring" and any(k in job_text for k in ["mentor", "lead", "senior", "manage"]):
            score += 0.15
        if entry.get("category") == "customer" and any(k in job_text for k in ["customer", "client", "stakeholder"]):
            score += 0.15
        if entry.get("category") == "presentation" and any(k in job_text for k in ["communic", "present", "stakeholder", "leadership"]):
            score += 0.1
        # Recency boost (entries from last 6 months get small boost)
        if entry.get("date", "").startswith("2026"):
            score += 0.05
        scored.append((entry, min(score, 1.0)))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [{"entry": e, "match_score": s} for e, s in scored if s > 0]


def _extract_entry_from_text(text: str) -> dict:
    """
    Extract structured entry from conversational input.
    Stub: simple heuristic. Live: Claude prompt.
    """
    if not claude_client.is_live():
        # Stub: take the first sentence as title, rest as description
        sentences = text.split(". ")
        title = sentences[0][:100] if sentences else text[:100]
        return {
            "title": title,
            "category": "project",  # default
            "description": text,
            "outcomes": [],
            "technologies": [],
            "stakeholders": [],
            "transferable_skills": [],
            "_stub": True,
        }

    prompt = (
        "Extract a structured journal entry from this work update. Return JSON:\n"
        "{ title, category (project|customer|tech_adoption|solution|mentoring|presentation|crisis_response|documentation|leadership), "
        "description, outcomes (quantified), technologies, stakeholders, transferable_skills }"
    )
    response = claude_client._call_claude(
        system=prompt,
        messages=[{"role": "user", "content": text}],
        max_tokens=512,
    )
    parsed = claude_client._parse_json_response(response, fallback={
        "title": text[:100], "category": "project", "description": text,
        "outcomes": [], "technologies": [], "stakeholders": [], "transferable_skills": [],
    })
    return parsed


def _stub_tailor(user_id, job_data, journal, matches):
    """Believable tailored resume even in stub mode."""
    top_matches = matches[:5]
    job_title = job_data.get("title", "Job")
    company = job_data.get("company", "Unknown")

    # Aggregate technologies + skills from top matches
    relevant_tech = set()
    relevant_skills = set()
    for m in top_matches:
        relevant_tech.update(m["entry"].get("technologies", []))
        relevant_skills.update(m["entry"].get("transferable_skills", []))

    # Build summary from top matches
    summary = (
        f"Senior Software Engineer with 4 years building production cloud infrastructure. "
        f"Track record includes shipping multi-region failover, leading critical incident response, "
        f"and adopting new platform technologies (Kubernetes, Service Mesh). "
        f"Strong cross-team coordination and mentoring experience. Match score for this role: "
        f"{int((sum(m['match_score'] for m in top_matches) / max(len(top_matches), 1)) * 100)}%."
    )

    return {
        "user_id": user_id,
        "job_url": job_data.get("url"),
        "job_title": job_title,
        "job_company": company,
        "tailored_at": datetime.utcnow().isoformat(),
        "match_score": round(sum(m["match_score"] for m in top_matches) / max(len(top_matches), 1), 2) if top_matches else 0,
        "tailored_summary": summary,
        "highlighted_projects": [
            {
                "title": m["entry"]["title"],
                "date": m["entry"]["date"],
                "category": m["entry"]["category"],
                "description": m["entry"]["description"],
                "outcomes": m["entry"].get("outcomes", []),
                "technologies": m["entry"].get("technologies", []),
                "match_score": round(m["match_score"], 2),
                "match_reason": _explain_match(m["entry"], job_data),
                "company": m["entry"].get("company", ""),
                "project": m["entry"].get("project", ""),
                # Endorsements travel with the project — the recruiter (and the
                # learner reviewing) sees who validated this work, with role
                # and comment. Only `approved` show; pending endorsements
                # would dilute the credibility signal.
                "endorsements": [
                    {
                        "endorser_name": en.get("endorser_name") or en.get("endorser_email"),
                        "endorser_role": en.get("endorser_role"),
                        "comment": en.get("comment", ""),
                        "endorsed_at": en.get("endorsed_at"),
                    }
                    for en in (m["entry"].get("endorsements") or [])
                    if en.get("status") == "approved"
                ],
            }
            for m in top_matches
        ],
        "key_outcomes_to_emphasize": [
            o for m in top_matches[:3] for o in m["entry"].get("outcomes", [])[:2]
        ][:6],
        "relevant_tech": sorted(relevant_tech)[:12],
        "transferable_skills": sorted(relevant_skills)[:8],
        "gaps_vs_job": _identify_gaps(job_data, journal),
        "experiences_to_emphasize": _suggest_emphasis(top_matches, job_data),
        "modes": {
            "computer": "live" if perplexity_client.is_live() else "stub",
            "classifier": "live" if claude_client.is_live() else "stub",
        },
        "_stub": True,
    }


def _explain_match(entry, job_data):
    job_text_lower = (job_data.get("title", "") + " " + job_data.get("raw_text", "")).lower()
    matched_techs = [t for t in entry.get("technologies", []) if t.lower() in job_text_lower]
    matched_skills = [s for s in entry.get("transferable_skills", []) if s.lower() in job_text_lower]
    parts = []
    if matched_techs:
        parts.append(f"Tech overlap: {', '.join(matched_techs[:3])}")
    if matched_skills:
        parts.append(f"Skill overlap: {', '.join(matched_skills[:2])}")
    if entry.get("category") in ("crisis_response", "leadership", "presentation"):
        parts.append(f"Category: {entry['category']} (often valued for senior roles)")
    return " · ".join(parts) if parts else "Pattern-matched to job description"


def _identify_gaps(job_data, journal):
    """What's the job asking for that the journal doesn't show? Stub: hardcoded common gaps."""
    return [
        "Direct people management experience (you've led projects but not had direct reports)",
        "Public conference talks (no entries in your journal show this)",
        "Patents or published papers (none in journal — add if relevant)",
    ]


def _suggest_emphasis(top_matches, job_data):
    if not top_matches:
        return []
    suggestions = []
    if any(m["entry"].get("category") == "crisis_response" for m in top_matches):
        suggestions.append("Lead with the Stripe outage incident response — it's your strongest credibility moment for senior infra roles.")
    if any(m["entry"].get("category") == "presentation" for m in top_matches):
        suggestions.append("Mention the VP Eng architecture review explicitly — shows executive communication, often a senior role differentiator.")
    if any("$" in str(o) for m in top_matches for o in m["entry"].get("outcomes", [])):
        suggestions.append("Quantify the cost-reporting tool savings ($12K/month) — recruiters skim for $ figures.")
    return suggestions or ["Customize the summary line to mirror the job's stated priorities."]


def _live_tailor(user_id, job_data, journal, matches):
    """
    Real Claude-powered tailoring.

    Reasons over the actual journal entries (not just keyword matches) to produce:
      - personalized summary grounded in the learner's track record
      - top 3-5 highlighted projects with explicit match reasoning
      - real gaps based on what the JD asks for vs what the journal shows
      - emphasis suggestions tied to specific journal entries

    The system prompt enforces: only cite outcomes / tech / skills that
    actually appear in the journal. Anything Claude can't ground gets
    listed under gaps_vs_job, never claimed.

    Falls back to _stub_tailor on any failure (empty journal, malformed
    JSON, API error) so the UI never sees a blank tailor response.
    """
    if not journal:
        logger.info("Resume tailor: journal empty for %s — falling back to stub", user_id)
        out = _stub_tailor(user_id, job_data, journal, matches)
        out["_stub_reason"] = "empty_journal"
        return out

    # Compact journal representation — keep input tokens manageable while
    # preserving every field the prompt grounds claims in.
    journal_compact = [
        {
            "entry_id": e.get("entry_id"),
            "date": e.get("date"),
            "title": e.get("title"),
            "category": e.get("category"),
            "description": (e.get("description") or "")[:600],
            "outcomes": e.get("outcomes") or [],
            "technologies": e.get("technologies") or [],
            "transferable_skills": e.get("transferable_skills") or [],
            "stakeholders": e.get("stakeholders") or [],
        }
        for e in journal
    ]

    job_title = job_data.get("title", "")
    job_company = job_data.get("company", "")
    job_text = (job_data.get("raw_text") or "")[:6000]

    system_prompt = (
        "You are a resume-tailoring assistant for a working software engineer. "
        "You produce job-specific tailored resume content that is GROUNDED IN THE LEARNER'S "
        "JOURNAL ENTRIES — never fabricated.\n\n"
        "ABSOLUTE RULES:\n"
        "  1. Every project, outcome, technology, and skill you cite MUST appear in a "
        "     journal entry below. If the JD asks for something the journal doesn't show, "
        "     list it under gaps_vs_job — do not claim the learner has it.\n"
        "  2. highlighted_projects entries MUST set entry_id to a real journal entry's "
        "     entry_id. Use the entry_id field exactly as given.\n"
        "  3. match_score values are floats 0.0-1.0. Calibrate honestly — most real JDs "
        "     should land 0.4-0.85 unless the journal is exceptionally strong/weak.\n"
        "  4. match_reason for each highlighted_project must cite the specific JD requirement "
        "     it addresses + the specific journal evidence (e.g. 'JD asks for incident "
        "     response leadership; entry j-003 shows 47-min Stripe outage recovery as IC').\n"
        "  5. Return ONLY a JSON object — no prose, no markdown fences.\n\n"
        "OUTPUT SHAPE (match exactly):\n"
        "{\n"
        '  "match_score": float 0-1,\n'
        '  "tailored_summary": "2-3 sentence elevator paragraph",\n'
        '  "highlighted_projects": [\n'
        '    {"entry_id": str, "title": str, "date": str, "category": str,\n'
        '     "description": str, "outcomes": [str], "technologies": [str],\n'
        '     "match_score": float 0-1, "match_reason": str}\n'
        "  ],\n"
        '  "key_outcomes_to_emphasize": [str],   // 4-6 quantified outcomes lifted verbatim\n'
        '  "relevant_tech": [str],                // tech in BOTH journal AND JD, 8-12 items\n'
        '  "transferable_skills": [str],          // soft skills journal evidences + JD asks for\n'
        '  "gaps_vs_job": [str],                  // 3-5 honest gaps, framed constructively\n'
        '  "experiences_to_emphasize": [str]      // 2-4 short tactical tips citing specific entries\n'
        "}\n\n"
        "Quality bar: a recruiter reading this output should be unable to tell which entries "
        "you're citing without being explicit. Be specific. No generic resume filler."
    )

    user_prompt = (
        f"JOB POSTING — {job_title}"
        + (f" at {job_company}" if job_company and job_company not in ("Unknown", "—") else "")
        + ":\n\n"
        f"{job_text or '(no JD body — tailor based on the role title alone)'}\n\n"
        "═══════════════════════════════════════════\n\n"
        f"LEARNER JOURNAL — {len(journal_compact)} entries, most recent first:\n\n"
        f"{json.dumps(journal_compact, indent=2)}"
    )

    try:
        # max_tokens=4096 — earlier 2048 caused output truncation on richer
        # JDs (5 highlighted projects with full match_reason + outcomes lists
        # easily push past 2K). Sonnet 4.5 happily handles up to 8K output;
        # 4096 is plenty of headroom while keeping latency bounded.
        response = claude_client._call_claude(
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
            max_tokens=4096,
            model=claude_client.DEFAULT_MODEL,
        )
        if not response:
            logger.warning("Resume tailor: Claude returned empty response for %s — falling back to stub", user_id)
            out = _stub_tailor(user_id, job_data, journal, matches)
            out["_stub_reason"] = "claude_empty_response"
            return out
        parsed = claude_client._parse_json_response(response, fallback=None)
        if parsed is None or not isinstance(parsed, dict):
            logger.warning(
                "Resume tailor: Claude returned malformed JSON for %s — first 200 chars: %r",
                user_id, (response or "")[:200],
            )
            out = _stub_tailor(user_id, job_data, journal, matches)
            out["_stub_reason"] = "claude_malformed_json"
            out["_claude_raw_preview"] = (response or "")[:300]
            return out

        # Defensive normalization — ensure every expected field is present
        # with a sane type. Claude generally complies but we never want a
        # KeyError on the frontend.
        highlighted = parsed.get("highlighted_projects") or []
        if not isinstance(highlighted, list):
            highlighted = []

        return {
            "user_id": user_id,
            "job_url": job_data.get("url"),
            "job_title": job_title,
            "job_company": job_company,
            "tailored_at": datetime.utcnow().isoformat(),
            "match_score": _coerce_float(parsed.get("match_score"), default=0.0),
            "tailored_summary": str(parsed.get("tailored_summary") or ""),
            "highlighted_projects": [
                {
                    "entry_id": p.get("entry_id"),
                    "title": str(p.get("title") or ""),
                    "date": str(p.get("date") or ""),
                    "category": str(p.get("category") or "project"),
                    "description": str(p.get("description") or ""),
                    "outcomes": list(p.get("outcomes") or []),
                    "technologies": list(p.get("technologies") or []),
                    "match_score": _coerce_float(p.get("match_score"), default=0.0),
                    "match_reason": str(p.get("match_reason") or ""),
                }
                for p in highlighted
                if isinstance(p, dict)
            ],
            "key_outcomes_to_emphasize": list(parsed.get("key_outcomes_to_emphasize") or []),
            "relevant_tech": list(parsed.get("relevant_tech") or []),
            "transferable_skills": list(parsed.get("transferable_skills") or []),
            "gaps_vs_job": list(parsed.get("gaps_vs_job") or []),
            "experiences_to_emphasize": list(parsed.get("experiences_to_emphasize") or []),
            "modes": {
                "computer": "live" if perplexity_client.is_live() else "stub",
                "classifier": "live",
            },
            "_stub": False,
        }
    except Exception as exc:
        logger.warning("Resume tailor live call failed for %s (%s) — falling back to stub", user_id, exc)
        out = _stub_tailor(user_id, job_data, journal, matches)
        out["_stub_reason"] = "claude_call_exception"
        out["_claude_error"] = f"{type(exc).__name__}: {exc}"
        return out


def _coerce_float(v, default: float = 0.0) -> float:
    """Defensive: Claude sometimes returns floats as strings or ints."""
    try:
        f = float(v)
        # Clamp to [0, 1] for match-score-like fields
        return max(0.0, min(1.0, f))
    except (TypeError, ValueError):
        return default
