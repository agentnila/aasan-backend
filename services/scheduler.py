"""
Scheduler — Project Manager Mode core logic (V3).

Three pieces:

  1. compute_free_slots() — pure: working hours − busy windows → free slots,
     filtered by duration, ranked by learner rhythm fit, top N.
  2. format_slots_for_chat() — shapes free slots into the {day, time, fit}
     payload the React CalendarSlotsCard expects.
  3. chunk_path_step() — Claude call: given a path step + available free
     windows + learner rhythm, propose how to slice the step into chunks
     that actually fit. Stubs to a deterministic split when Claude is offline.

WORKING HOURS BY RHYTHM
───────────────────────
  morning:   07:00 – 12:00 (best slots earlier)
  afternoon: 12:00 – 17:00
  evening:   17:00 – 21:00
  default:   09:00 – 17:00 (typical workday)

Rhythm comes from Mem0 preferences (key: `learning_rhythm`). When unknown,
default is used and the find-slots response includes a hint to set it.
"""

from datetime import datetime, timedelta, timezone
from services import calendar_client


RHYTHM_HOURS = {
    "morning":   (7, 12),
    "afternoon": (12, 17),
    "evening":   (17, 21),
    "default":   (9, 17),
}

DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Multi-goal weighting (Phase C)
PRIORITY_WEIGHTS = {"primary": 3.0, "secondary": 2.0, "exploration": 1.0,
                    1: 3.0, 2: 2.0, 3: 1.0,
                    "1": 3.0, "2": 2.0, "3": 1.0}


# ──────────────────────────────────────────────────────────────
# Slot computation
# ──────────────────────────────────────────────────────────────

def compute_free_slots(
    user_id: str,
    duration_min: int = 30,
    count: int = 3,
    window_start: datetime = None,
    window_end: datetime = None,
    rhythm: str = "default",
    goals: list = None,
) -> list:
    """
    Returns a list of free-slot dicts, ranked best-fit first.

    Each slot dict:
      {
        "start": ISO8601,
        "end":   ISO8601,
        "day":   "Today" | "Tomorrow" | "Mon" | ...,
        "time":  "9:00 – 9:30 AM",
        "fit":   one-line explanation tied to rhythm + surroundings,
        "score": 0.0–1.0,
        "goal_id": str | None,   # populated when goals provided
        "goal_name": str | None,
      }

    When `goals` is provided (V3 multi-goal), slots are tagged with the goal
    they should serve, weighted by priority × urgency × inverse_progress.
    See compute_goal_budget() for the weight formula.
    """
    now = datetime.now(timezone.utc)
    window_start = window_start or now
    window_end = window_end or (now + timedelta(days=7))

    busy = calendar_client.list_busy_windows(user_id, window_start, window_end)
    work_start_h, work_end_h = RHYTHM_HOURS.get(rhythm, RHYTHM_HOURS["default"])

    candidates = []
    day = window_start.date()
    end_day = window_end.date()
    while day <= end_day:
        if day.weekday() < 5:  # weekdays only
            day_start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=work_start_h)
            day_end = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=work_end_h)
            if day_end > now:
                free = _subtract_busy(max(day_start, now), day_end, busy)
                for free_start, free_end in free:
                    candidates.extend(_slice_slot(free_start, free_end, duration_min))
        day += timedelta(days=1)

    today = now.date()
    ranked = sorted(
        ({**c, **_score_slot(c, rhythm, today)} for c in candidates),
        key=lambda c: (-c["score"], c["start"]),
    )
    top = ranked[:count]
    serialized = [_serialize_slot(c, today) for c in top]
    if goals:
        _assign_goals_to_slots(serialized, goals)
    return serialized


def _subtract_busy(window_start, window_end, busy):
    """Remove busy windows from [window_start, window_end). Returns free intervals."""
    free = [(window_start, window_end)]
    for b_start, b_end in busy:
        next_free = []
        for f_start, f_end in free:
            if b_end <= f_start or b_start >= f_end:
                next_free.append((f_start, f_end))
                continue
            if b_start > f_start:
                next_free.append((f_start, b_start))
            if b_end < f_end:
                next_free.append((b_end, f_end))
        free = next_free
    return free


def _slice_slot(start, end, duration_min):
    """A single free interval may host multiple candidate slots; emit the obvious starts."""
    out = []
    duration = timedelta(minutes=duration_min)
    cursor = start
    while cursor + duration <= end:
        out.append({"start": cursor, "end": cursor + duration})
        cursor += timedelta(minutes=30)
    return out


def _score_slot(slot, rhythm, today):
    start = slot["start"]
    score = 0.5
    hour = start.hour
    if rhythm == "morning" and hour <= 10: score += 0.3
    elif rhythm == "afternoon" and 13 <= hour <= 15: score += 0.3
    elif rhythm == "evening" and hour >= 17: score += 0.3
    elif rhythm == "default" and 9 <= hour <= 11: score += 0.2
    days_out = (start.date() - today).days
    score += max(0, 0.2 - 0.05 * days_out)
    return {"score": round(min(1.0, score), 3)}


def _serialize_slot(slot, today):
    start, end = slot["start"], slot["end"]
    delta_days = (start.date() - today).days
    if delta_days == 0:
        day = "Today"
    elif delta_days == 1:
        day = "Tomorrow"
    else:
        day = DAY_LABELS[start.weekday()]
    fit = _fit_explanation(start)
    return {
        "start": start.isoformat(),
        "end":   end.isoformat(),
        "day":   day,
        "time":  _fmt_time_range(start, end),
        "fit":   fit,
        "score": slot["score"],
    }


def _fmt_time_range(start, end):
    fmt = lambda t: t.strftime("%-I:%M %p").replace(":00 ", " ")
    return f"{fmt(start)} – {fmt(end)}"


def _fit_explanation(start):
    h = start.hour
    if h < 10: return "Start your day with learning — fresh mind"
    if h < 12: return "Mid-morning slot — between standup and lunch"
    if 12 <= h < 14: return "Lunch break slot — quick and focused"
    if 14 <= h < 16: return "Afternoon slot — steady focus window"
    return "End-of-day slot — wind-down learning"


# ──────────────────────────────────────────────────────────────
# Multi-goal weighting (Phase C)
# ──────────────────────────────────────────────────────────────

def compute_goal_budget(goals: list, total_minutes_per_week: int = 300) -> list:
    """
    Given a learner's active goals, return how many minutes/week each should
    receive. Weight formula:

        weight = priority_weight × urgency_weight × progress_factor

    Where:
      - priority_weight: primary=3, secondary=2, exploration=1
      - urgency_weight:  weeks_remaining ≤ 2 → 2.0
                         weeks_remaining ≤ 6 → 1.5
                         past deadline       → 3.0
                         else                → 1.0
      - progress_factor: max(0.5, 1 - progress_pct/100) — laggards get more

    Returns a list of {goal_id, name, weight, share, minutes_per_week} rows
    sorted by weight desc. Surfaces in the Goals Dashboard.
    """
    now = datetime.now(timezone.utc).date()
    rows = []
    for g in goals or []:
        gid = g.get("goal_id") or g.get("id") or g.get("name") or "unknown"
        name = g.get("name") or g.get("goal") or gid
        priority = g.get("priority", "secondary")
        priority_w = PRIORITY_WEIGHTS.get(priority, PRIORITY_WEIGHTS["secondary"])

        urgency_w = 1.0
        deadline_str = g.get("deadline") or g.get("timeline")
        if deadline_str:
            try:
                deadline = datetime.fromisoformat(str(deadline_str)[:10]).date()
                days = (deadline - now).days
                if days < 0:
                    urgency_w = 3.0
                elif days <= 14:
                    urgency_w = 2.0
                elif days <= 42:
                    urgency_w = 1.5
            except Exception:
                pass

        progress_pct = float(g.get("progress_pct") or 0)
        progress_f = max(0.5, 1.0 - progress_pct / 100.0)

        weight = priority_w * urgency_w * progress_f
        rows.append({
            "goal_id": gid,
            "name": name,
            "priority": priority,
            "weight": round(weight, 3),
        })

    total_w = sum(r["weight"] for r in rows) or 1.0
    for r in rows:
        r["share"] = round(r["weight"] / total_w, 3)
        r["minutes_per_week"] = round(r["share"] * total_minutes_per_week)
    rows.sort(key=lambda r: -r["weight"])
    return rows


def _assign_goals_to_slots(slots: list, goals: list) -> None:
    """
    In-place: tag each slot with the goal it should serve. Allocates by share
    using the largest-remainder method so totals match share intent for small N.
    """
    budget = compute_goal_budget(goals)
    if not budget or not slots:
        return

    n = len(slots)
    # Largest remainder allocation
    raw = [(b, b["share"] * n) for b in budget]
    base = [(b, int(quota)) for b, quota in raw]
    used = sum(q for _, q in base)
    remainders = sorted(
        ((b, raw[i][1] - base[i][1]) for i, (b, _) in enumerate(base)),
        key=lambda x: -x[1],
    )
    allocations = {b["goal_id"]: q for b, q in base}
    leftover = n - used
    for i in range(leftover):
        gid = remainders[i % len(remainders)][0]["goal_id"]
        allocations[gid] += 1

    # Walk slots in order, hand them to goals weighted-most-first
    queue = []
    by_id = {b["goal_id"]: b for b in budget}
    for gid, q in sorted(allocations.items(), key=lambda kv: -by_id[kv[0]]["weight"]):
        queue.extend([gid] * q)

    for slot, gid in zip(slots, queue):
        b = by_id[gid]
        slot["goal_id"] = gid
        slot["goal_name"] = b["name"]


# ──────────────────────────────────────────────────────────────
# Auto-chunking — Claude proposes how to slice a step
# ──────────────────────────────────────────────────────────────

def chunk_path_step(step_title: str, estimated_minutes: int, available_windows: list, rhythm: str = "default") -> dict:
    """
    Given a path step that's larger than any single available window, propose
    a chunking strategy. Returns:

      {
        "chunks": [{"title": "...", "minutes": 25}, ...],
        "rationale": "one-line explanation",
        "mode": "claude" | "stub",
      }

    Phase A: deterministic split (25-min Pomodoro-ish slices). Phase B: real
    Claude call so chunks respect natural boundaries in the content.
    """
    from services import claude_client

    if not claude_client.is_live() or not estimated_minutes:
        return _stub_chunking(step_title, estimated_minutes or 60)

    system = (
        "You are a learning coach. Given a learning step, its estimated total minutes, "
        "and the time slots available, propose a chunking that respects natural content "
        "boundaries (concepts, modules, exercises). Each chunk should be 15–45 min. "
        "Return ONLY JSON: {\"chunks\": [{\"title\": str, \"minutes\": int}], \"rationale\": str}"
    )
    user = (
        f"Step: {step_title}\n"
        f"Estimated total: {estimated_minutes} min\n"
        f"Learner rhythm: {rhythm}\n"
        f"Available window sizes: {[w.get('duration_min') for w in available_windows]}"
    )
    try:
        raw = claude_client._call_claude(system, [{"role": "user", "content": user}], max_tokens=512)
        parsed = claude_client._parse_json_response(raw, fallback={})
        if parsed.get("chunks"):
            return {**parsed, "mode": "claude"}
    except Exception:
        pass
    return _stub_chunking(step_title, estimated_minutes)


def _stub_chunking(step_title, total_minutes):
    chunk_size = 25 if total_minutes >= 50 else total_minutes
    n = max(1, total_minutes // chunk_size)
    chunks = [
        {"title": f"{step_title} — part {i+1} of {n}", "minutes": chunk_size}
        for i in range(n)
    ]
    remainder = total_minutes - chunk_size * n
    if remainder >= 10:
        chunks.append({"title": f"{step_title} — wrap-up", "minutes": remainder})
    return {
        "chunks": chunks,
        "rationale": f"Split into {len(chunks)} {chunk_size}-min focus blocks.",
        "mode": "stub",
    }


# ──────────────────────────────────────────────────────────────
# Conflict detection — used by /calendar/reschedule
# ──────────────────────────────────────────────────────────────

def detect_conflicts(blocks: list, busy_windows: list) -> list:
    """
    Given active Schedule_Blocks and freshly-fetched busy windows from
    Calendar, return blocks whose start_at..end_at overlaps a busy window
    that wasn't there at booking time.
    """
    conflicts = []
    for block in blocks:
        b_start = datetime.fromisoformat(block["start_at"])
        b_end = datetime.fromisoformat(block["end_at"])
        for busy_start, busy_end in busy_windows:
            if b_start < busy_end and b_end > busy_start:
                conflicts.append({**block, "conflict_with": [busy_start.isoformat(), busy_end.isoformat()]})
                break
    return conflicts
