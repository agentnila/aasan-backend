-- ============================================================================
-- 0003 · Schedule blocks (path step → calendar event linkage) — 2026-05-02
-- ============================================================================
-- A learner who plans 12 steps without a way to actually time-block them
-- into their week never gets started. This table is the bridge between
-- the abstract path step ("Set up Linux dev env, 45 min") and a concrete
-- calendar event ("Tuesday 3:00-3:45 PM, Google Calendar event abc123").
--
-- Mirrors V2 Data Model Section 2 Table 18. Composite (user_id, block_id)
-- so we never collide across users; calendar_event_id is the source of
-- truth for the underlying Google Calendar event when integration is live.
-- When integration is in stub mode, calendar_event_id is the stub ID
-- returned by services/calendar_client.py and the event isn't really on
-- a real calendar — but the row is still useful as the user's plan.
--
-- Status workflow (matches the V2 spec):
--   scheduled  → in_progress → completed   (happy path)
--                            ↓
--                            missed         (window passed without engagement)
--   any        → rescheduled                (moved to a new time)
--   any        → cancelled                  (deleted)
--
-- Idempotent — re-runnable.
-- ============================================================================

CREATE TABLE IF NOT EXISTS schedule_blocks (
    block_id              bigserial    PRIMARY KEY,
    user_id               text         NOT NULL,
    goal_id               text,                                                -- nullable; not every block ties to a goal
    path_step_id          text,                                                -- nullable; not every block ties to a step (e.g. SME session)
    step_title            text         NOT NULL,                               -- snapshotted; survives even if step is later renamed/removed
    start_at              timestamptz  NOT NULL,
    end_at                timestamptz  NOT NULL,
    duration_minutes      int          GENERATED ALWAYS AS (
                                          GREATEST(0, EXTRACT(EPOCH FROM (end_at - start_at))::int / 60)
                                       ) STORED,
    calendar_event_id     text,                                                -- Google Calendar event ID; nullable when stub
    calendar_event_url    text,                                                -- direct link to the calendar event
    status                text         NOT NULL DEFAULT 'scheduled'
        CHECK (status IN ('scheduled', 'in_progress', 'completed', 'missed', 'rescheduled', 'cancelled')),
    nudge_at              timestamptz,                                         -- when the 5-min-prior nudge fires
    nudge_sent_at         timestamptz,                                         -- set when nudge has fired
    reschedule_count      int          NOT NULL DEFAULT 0,
    original_start_at     timestamptz  NOT NULL,                               -- audit trail: first booked time
    description           text,
    mode                  text,                                                -- 'live' | 'stub' — provenance from calendar_client
    created_at            timestamptz  NOT NULL DEFAULT now(),
    updated_at            timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_schedule_blocks_user             ON schedule_blocks (user_id, start_at);
CREATE INDEX IF NOT EXISTS idx_schedule_blocks_user_status      ON schedule_blocks (user_id, status);
CREATE INDEX IF NOT EXISTS idx_schedule_blocks_path_step        ON schedule_blocks (user_id, goal_id, path_step_id) WHERE path_step_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_schedule_blocks_nudge_due        ON schedule_blocks (nudge_at) WHERE nudge_sent_at IS NULL AND status = 'scheduled';

DROP TRIGGER IF EXISTS schedule_blocks_set_updated_at ON schedule_blocks;
CREATE TRIGGER schedule_blocks_set_updated_at
    BEFORE UPDATE ON schedule_blocks
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Sentinel
INSERT INTO schema_migrations (version) VALUES ('0003_schedule_blocks')
    ON CONFLICT (version) DO NOTHING;
