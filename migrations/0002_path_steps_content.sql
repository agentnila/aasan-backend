-- ============================================================================
-- 0002 · Path step content links (2026-05-02)
-- ============================================================================
-- A path step that's just a title is a dead-end UX — the learner can't
-- actually click through to engage with the content. This migration adds
-- the three fields the Path Engine and frontend need to surface a real
-- learning resource per step.
--
-- All columns are nullable + ADD IF NOT EXISTS so the migration is
-- idempotent and won't break existing rows. Older steps without content
-- links just don't render the "Open content ↗" CTA on the frontend.
--
-- The longer-term home for content metadata is the Content_Index table
-- (V2 Data Model Section 2 Table 03), and `content_id` will reference
-- that table once it lands. Until then, we embed the resource metadata
-- directly on the step row.
-- ============================================================================

ALTER TABLE path_steps
    ADD COLUMN IF NOT EXISTS content_url      text,        -- direct URL to the resource
    ADD COLUMN IF NOT EXISTS content_provider text,        -- "Coursera" | "AWS Skill Builder" | "kubernetes.io" | "YouTube" | etc.
    ADD COLUMN IF NOT EXISTS content_title    text,        -- the resource's own title (may differ from step title)
    ADD COLUMN IF NOT EXISTS content_id       text;        -- forward-compat: future FK to content_index.content_id

-- Sentinel insert
INSERT INTO schema_migrations (version) VALUES ('0002_path_steps_content')
    ON CONFLICT (version) DO NOTHING;
