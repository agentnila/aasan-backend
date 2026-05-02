-- ============================================================================
-- 0006 · Path phases — group steps under titled, sequenced phases (2026-05-02)
-- ============================================================================
-- L4a — RAG-augmented path generation. Where 0002 made path_steps catalog-aware
-- (content_url, content_id), 0006 lifts the structure: a path is now a sequence
-- of phases (3-6 typical), each phase has its own title, duration, rationale,
-- and deliverable. Steps are still flat in path_steps but tagged with a
-- phase_local_id linking back here.
--
-- phase_local_id is a stable string ("phase-1", "phase-2", ...) rather than
-- a numeric FK, because path regeneration deletes-and-recreates everything in
-- one transaction and a string ID makes the regen logic simpler. UNIQUE is
-- per (user_id, goal_id, phase_local_id).
--
-- Backwards-compatible: existing path_steps without phase_local_id render as
-- a flat list (legacy mode). New paths populate both tables together.
--
-- step_rationale on path_steps: short reason this resource was chosen for this
-- phase (e.g. "Builds the prompting baseline you'll need for Phase 3 framework
-- work"). Comes from Claude during RAG generation.
--
-- is_free on path_steps: derived from the catalog row's is_free at materialization
-- time. Lets the path-variants work in L4c filter steps without re-joining.
--
-- Idempotent — safe to re-run.
-- ============================================================================

CREATE TABLE IF NOT EXISTS path_phases (
    phase_id         bigserial PRIMARY KEY,
    user_id          text         NOT NULL,
    goal_id          text         NOT NULL,
    phase_local_id   text         NOT NULL,
    order_index      int          NOT NULL,
    title            text         NOT NULL,
    duration_weeks   int,
    rationale_md     text,
    deliverable_md   text,
    created_at       timestamptz  NOT NULL DEFAULT now(),
    UNIQUE (user_id, goal_id, phase_local_id)
);

CREATE INDEX IF NOT EXISTS idx_path_phases_goal
    ON path_phases (user_id, goal_id, order_index);

ALTER TABLE path_steps
    ADD COLUMN IF NOT EXISTS phase_local_id text,
    ADD COLUMN IF NOT EXISTS step_rationale text,
    ADD COLUMN IF NOT EXISTS is_free        boolean DEFAULT true;

CREATE INDEX IF NOT EXISTS idx_path_steps_phase
    ON path_steps (user_id, goal_id, phase_local_id);

INSERT INTO schema_migrations (version) VALUES ('0006_path_phases')
    ON CONFLICT (version) DO NOTHING;
