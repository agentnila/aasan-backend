-- ============================================================================
-- 0005 · Content Index — the unified catalog of all learning content (2026-05-02)
-- ============================================================================
-- Mirrors V2 Data Model Section 2 Table 03. The "Google index" of enterprise
-- learning. Path Engine reads from here when generating paths (RAG: query
-- Pinecone → retrieve top candidates → Claude SELECTS from them + writes
-- narrative). LibraryCanvas reads from here for read-only browse.
--
-- Idempotency on (source, external_id) — re-uploading the same row updates
-- in place, never duplicates. external_id is whatever ID the source LMS / API
-- assigns; if missing at import time, services/content_index.py synthesizes
-- from slug(source + title).
--
-- Aasan-managed fields (embedding_id, quality_score, indexed_at, last_synced_at)
-- are populated by background jobs / admin actions; CSV imports leave them null
-- and the embed_pending route fills them in.
--
-- skills + prerequisites are text[] for GIN-indexed contains queries:
--   SELECT * FROM content_index WHERE skills && ARRAY['langchain', 'rag'];
--
-- Idempotent — safe to re-run.
-- ============================================================================

CREATE TABLE IF NOT EXISTS content_index (
    content_id        bigserial    PRIMARY KEY,
    external_id       text,                                         -- source-system ID
    source            text         NOT NULL,                        -- "Coursera" | "Cloudera" | "AWS Skill Builder" | "YouTube" | etc.
    title             text         NOT NULL,
    source_url        text         NOT NULL,
    content_type      text         NOT NULL DEFAULT 'course'
        CHECK (content_type IN ('course', 'video', 'article', 'lab', 'quiz', 'pdf', 'book', 'slides', 'interactive', 'other')),
    duration_minutes  int          NOT NULL DEFAULT 60,
    description       text,
    skills            text[]       NOT NULL DEFAULT '{}',
    prerequisites     text[]       NOT NULL DEFAULT '{}',
    difficulty        text         CHECK (difficulty IN ('beginner', 'intermediate', 'advanced', 'expert')),
    is_free           boolean      NOT NULL DEFAULT true,
    language          text         NOT NULL DEFAULT 'en',
    -- Aasan-managed
    embedding_id      text,                                         -- Pinecone vector ID; NULL until embed pipeline runs
    quality_score     numeric(3,2),                                 -- 0-1, optional Claude-rated quality
    indexed_at        timestamptz  NOT NULL DEFAULT now(),
    last_synced_at    timestamptz  NOT NULL DEFAULT now(),
    -- Provenance
    imported_by       text,                                         -- actor user_id who uploaded (audit)
    contributed_by    text,                                         -- if a manager/learner contributed (different from import bot)
    UNIQUE (source, external_id)
);

CREATE INDEX IF NOT EXISTS idx_content_skills        ON content_index USING GIN (skills);
CREATE INDEX IF NOT EXISTS idx_content_prereqs       ON content_index USING GIN (prerequisites);
CREATE INDEX IF NOT EXISTS idx_content_source        ON content_index (source);
CREATE INDEX IF NOT EXISTS idx_content_difficulty    ON content_index (difficulty);
CREATE INDEX IF NOT EXISTS idx_content_free          ON content_index (is_free) WHERE is_free = true;
CREATE INDEX IF NOT EXISTS idx_content_unindexed     ON content_index (content_id) WHERE embedding_id IS NULL;

INSERT INTO schema_migrations (version) VALUES ('0005_content_index')
    ON CONFLICT (version) DO NOTHING;
