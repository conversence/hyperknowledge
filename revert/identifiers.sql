-- Revert identifiers

BEGIN;

DROP INDEX IF EXISTS vocabulary_idx;
DROP INDEX IF EXISTS vocabulary_prefix_idx;
DROP INDEX IF EXISTS term_idx;
DROP INDEX IF EXISTS uuterm_idx;

DROP TABLE IF EXISTS public.uuidentifier;
DROP TABLE IF EXISTS public.term;
DROP TABLE IF EXISTS public.vocabulary;

COMMIT;
