-- ============================================================================
-- 0004 · Goal context (URL, document, image attached to goal) — 2026-05-02
-- ============================================================================
-- Goals can optionally carry rich context — a target job posting URL, a
-- pasted JD, a PDF / Word doc with role description, a screenshot of a
-- LinkedIn role. The Path Engine uses this context to ground the
-- generated path in the actual role/objective rather than guessing from
-- a one-line goal name.
--
-- Single context-set per goal for now. context_source_type tracks the
-- input modality so the UI knows which icon/badge to render. Whatever
-- the source, context_text is the extracted text passed to Claude during
-- path generation; the original URL/filename is kept for provenance.
--
-- Idempotent — safe to re-run.
-- ============================================================================

ALTER TABLE goals
    ADD COLUMN IF NOT EXISTS context_source_type text
        CHECK (context_source_type IN ('none', 'url', 'document', 'image', 'text')),
    ADD COLUMN IF NOT EXISTS context_url      text,
    ADD COLUMN IF NOT EXISTS context_filename text,
    ADD COLUMN IF NOT EXISTS context_mime     text,
    ADD COLUMN IF NOT EXISTS context_text     text;       -- extracted/normalized text fed to Claude

INSERT INTO schema_migrations (version) VALUES ('0004_goal_context')
    ON CONFLICT (version) DO NOTHING;
