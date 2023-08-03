-- Revert artefacts


BEGIN;

DROP INDEX IF EXISTS topic_has_projections_idx;
DROP TABLE IF EXISTS public.topic;

COMMIT;
