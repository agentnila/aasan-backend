-- ============================================================================
-- Aasan — Tier 0 initial schema (2026-05-01)
-- ============================================================================
-- Translates Aasan_V2_Data_Model.md Section 2 (information model) into Postgres
-- DDL. Tier 0 scope: goals, paths, path_steps, path_recomputes, journal_entries,
-- work_items, audit_log. Other tables in the V2 information model (employees,
-- content_index, skills, etc.) come in later tiers.
--
-- Idempotent: every CREATE uses IF NOT EXISTS. Safe to re-run on an existing
-- database. Subsequent migrations live in 0002_*.sql, 0003_*.sql, etc.
--
-- Conventions:
--   * Surrogate keys: bigserial (or text where natural key is meaningful, like
--     goal slugs and path step IDs that the path engine generates).
--   * user_id: text — Clerk user IDs are strings (e.g. "user_2abc..."), and
--     the demo seed uses "demo-user". Not a foreign key (employees table
--     comes in a later tier).
--   * Timestamps: timestamptz with DEFAULT now().
--   * Soft-delete: status enums include "deleted" or "archived" where
--     applicable. No hard deletes from app code.
--   * RLS: not enabled in this migration (single-tenant pilot). Add later
--     when multi-tenancy lands. Service role bypasses RLS regardless.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- Helper: updated_at trigger
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- 02. GOALS  (Aasan_V2_Data_Model.md Table 02)
-- ============================================================================
CREATE TABLE IF NOT EXISTS goals (
    user_id            text        NOT NULL,
    goal_id            text        NOT NULL,                   -- slug, e.g. "cloud-architect"
    name               text        NOT NULL,
    objective          text,
    timeline           text,                                   -- free-form: ISO date OR label like "Q4 2026"
    days_left          int,
    success_criteria   text,
    priority           text        NOT NULL DEFAULT 'secondary'
        CHECK (priority IN ('primary', 'secondary', 'exploration', 'assigned')),
    status             text        NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'achieved', 'paused', 'abandoned', 'archived')),
    readiness          int         NOT NULL DEFAULT 0
        CHECK (readiness BETWEEN 0 AND 100),
    delta              text,                                   -- short label, e.g. "+10 this wk"
    assigned_by        text        NOT NULL DEFAULT 'self'
        CHECK (assigned_by IN ('self', 'manager', 'ld_admin')),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, goal_id)
);

CREATE INDEX IF NOT EXISTS idx_goals_user_status ON goals (user_id, status);
CREATE INDEX IF NOT EXISTS idx_goals_priority   ON goals (priority) WHERE status = 'active';

DROP TRIGGER IF EXISTS goals_set_updated_at ON goals;
CREATE TRIGGER goals_set_updated_at
    BEFORE UPDATE ON goals
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================================
-- 16. PATHS  (Aasan_V2_Data_Model.md Table 16 — Learning_Paths)
-- ============================================================================
CREATE TABLE IF NOT EXISTS paths (
    user_id                  text        NOT NULL,
    goal_id                  text        NOT NULL,
    path_id                  text        NOT NULL,             -- e.g. "path-cloud-architect"
    title                    text        NOT NULL,
    progress_pct             int         NOT NULL DEFAULT 0
        CHECK (progress_pct BETWEEN 0 AND 100),
    current_step_id          text,
    estimated_total_minutes  int         NOT NULL DEFAULT 0,
    last_recompute_reason    text,
    last_recomputed_at       timestamptz,
    status                   text        NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'paused', 'completed', 'abandoned')),
    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, goal_id),
    FOREIGN KEY (user_id, goal_id) REFERENCES goals(user_id, goal_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_paths_user ON paths (user_id);

DROP TRIGGER IF EXISTS paths_set_updated_at ON paths;
CREATE TRIGGER paths_set_updated_at
    BEFORE UPDATE ON paths
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================================
-- 17. PATH_STEPS  (Aasan_V2_Data_Model.md Table 17)
-- ============================================================================
CREATE TABLE IF NOT EXISTS path_steps (
    user_id                text        NOT NULL,
    goal_id                text        NOT NULL,
    step_id                text        NOT NULL,               -- e.g. "step-1", "step-5a"
    step_order             numeric(6,2) NOT NULL,              -- numeric to allow inserts at 5.5
    title                  text        NOT NULL,
    step_type              text        NOT NULL DEFAULT 'content'
        CHECK (step_type IN ('content', 'review', 'refresher', 'gap_closure', 'assignment', 'synthetic')),
    status                 text        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'active', 'done', 'known', 'skipped', 'stale')),
    estimated_minutes      int,
    actual_minutes         int,
    mastery_at_completion  numeric(4,3) CHECK (mastery_at_completion BETWEEN 0 AND 1),
    inserted_by            text        DEFAULT 'engine'
        CHECK (inserted_by IN ('engine', 'learner', 'manager', 'assignment')),
    inserted_reason        text,
    completed_at           timestamptz,
    inserted_at            timestamptz NOT NULL DEFAULT now(),
    updated_at             timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, goal_id, step_id),
    FOREIGN KEY (user_id, goal_id) REFERENCES paths(user_id, goal_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_path_steps_order   ON path_steps (user_id, goal_id, step_order);
CREATE INDEX IF NOT EXISTS idx_path_steps_status  ON path_steps (user_id, goal_id, status);

DROP TRIGGER IF EXISTS path_steps_set_updated_at ON path_steps;
CREATE TRIGGER path_steps_set_updated_at
    BEFORE UPDATE ON path_steps
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================================
-- PATH_RECOMPUTES  (the recompute_history JSON column from Table 16, normalized
-- into rows for proper SQL queryability — "show me every path adjustment in
-- the last 30 days" becomes a simple WHERE clause instead of jsonb_agg.)
-- ============================================================================
CREATE TABLE IF NOT EXISTS path_recomputes (
    recompute_id  bigserial     PRIMARY KEY,
    user_id       text          NOT NULL,
    goal_id       text          NOT NULL,
    recomputed_at timestamptz   NOT NULL DEFAULT now(),
    trigger       text          NOT NULL,                     -- session_complete, content_added, staleness_flag, assignment_create, learner_edit
    reason        text,
    diff          jsonb         NOT NULL DEFAULT '{}'::jsonb, -- {added: [...], removed: [...], reordered: [...]}
    FOREIGN KEY (user_id, goal_id) REFERENCES paths(user_id, goal_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_path_recomputes_user_goal ON path_recomputes (user_id, goal_id, recomputed_at DESC);
CREATE INDEX IF NOT EXISTS idx_path_recomputes_trigger   ON path_recomputes (trigger);


-- ============================================================================
-- 26. JOURNAL_ENTRIES  (Aasan_V2_Data_Model.md Table 26 — Resume_Journal)
-- ============================================================================
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id              bigserial   PRIMARY KEY,
    user_id               text        NOT NULL,
    entry_external_id     text,                                -- the human-meaningful ID like "j-001"; nullable
    entry_date            date        NOT NULL DEFAULT CURRENT_DATE,
    title                 text        NOT NULL,
    category              text        NOT NULL DEFAULT 'project'
        CHECK (category IN ('project', 'customer', 'tech_adoption', 'mentoring', 'presentation',
                            'crisis_response', 'documentation', 'leadership', 'solution', 'other')),
    description           text,
    outcomes              jsonb       NOT NULL DEFAULT '[]'::jsonb,   -- list of strings
    technologies          text[]      NOT NULL DEFAULT '{}',
    stakeholders          jsonb       NOT NULL DEFAULT '[]'::jsonb,   -- list of strings
    transferable_skills   text[]      NOT NULL DEFAULT '{}',
    raw_input             text,
    -- Resume Module social layer (Phase 3 — endorsements + share)
    company               text        NOT NULL DEFAULT '',
    project               text        NOT NULL DEFAULT '',
    author_id             text,                                -- typically same as user_id
    endorsements          jsonb       NOT NULL DEFAULT '[]'::jsonb,   -- list of {endorser_email, endorser_name, endorser_role, status, requested_at, endorsed_at, comment}
    shared_with           jsonb       NOT NULL DEFAULT '[]'::jsonb,   -- list of peer email strings
    captured_at           timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (user_id, entry_external_id)                        -- nullable column; only enforced when set
);

CREATE INDEX IF NOT EXISTS idx_journal_user_date     ON journal_entries (user_id, entry_date DESC);
CREATE INDEX IF NOT EXISTS idx_journal_user_category ON journal_entries (user_id, category);

DROP TRIGGER IF EXISTS journal_set_updated_at ON journal_entries;
CREATE TRIGGER journal_set_updated_at
    BEFORE UPDATE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================================
-- 27. WORK_ITEMS  (Aasan_V2_Data_Model.md Table 27 — granular build tracker)
-- ============================================================================
CREATE TABLE IF NOT EXISTS work_items (
    work_item_id      bigserial    PRIMARY KEY,
    title             text         NOT NULL,
    status            text         NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_progress', 'completed', 'blocked', 'deleted')),
    description       text         NOT NULL DEFAULT '',
    owner             text         NOT NULL DEFAULT 'balaji',
    parent_ship_date  date,                                    -- links to BUILD_LOG.md ## YYYY-MM-DD
    tags              text[]       NOT NULL DEFAULT '{}',
    estimated_minutes int,
    actual_minutes    int,
    created_at        timestamptz  NOT NULL DEFAULT now(),
    updated_at        timestamptz  NOT NULL DEFAULT now(),
    completed_at      timestamptz                              -- set on transition → completed; NULL otherwise
);

CREATE INDEX IF NOT EXISTS idx_work_items_status      ON work_items (status) WHERE status != 'deleted';
CREATE INDEX IF NOT EXISTS idx_work_items_owner       ON work_items (owner);
CREATE INDEX IF NOT EXISTS idx_work_items_parent_ship ON work_items (parent_ship_date) WHERE parent_ship_date IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_work_items_tags        ON work_items USING GIN (tags);

DROP TRIGGER IF EXISTS work_items_set_updated_at ON work_items;
CREATE TRIGGER work_items_set_updated_at
    BEFORE UPDATE ON work_items
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


-- ============================================================================
-- AUDIT_LOG  (new — append-only trail for admin actions.
-- Promotes the spec-only "Phase C audit log" entry in BUILD_LOG.md into a real
-- backing table. A future services/audit_log.py + /admin/audit/* endpoints can
-- read/write this directly.)
-- ============================================================================
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id      text        PRIMARY KEY,                    -- "a-{epoch_ms}-{seq}" generated by services/audit_log.record()
    occurred_at   timestamptz NOT NULL DEFAULT now(),
    actor_user_id text        NOT NULL,                       -- who took the action
    actor_role    text,                                       -- snapshotted at action time (admin, manager, learner)
    action        text        NOT NULL,                       -- namespaced, e.g. "admin:role_change", "scim:user_create"
    target        text,                                       -- e.g. "user:jordan-lee", "goal:cloud-architect"
    details       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    request_id    text                                        -- optional correlation ID
);

CREATE INDEX IF NOT EXISTS idx_audit_log_actor    ON audit_log (actor_user_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_action   ON audit_log (action, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_occurred ON audit_log (occurred_at DESC);


-- ============================================================================
-- Migration completion sentinel (lets services/db.py confirm the schema is
-- present without inspecting individual tables).
-- ============================================================================
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     text         PRIMARY KEY,
    applied_at  timestamptz  NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (version) VALUES ('0001_init')
    ON CONFLICT (version) DO NOTHING;
