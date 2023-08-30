-- Deploy context_history


BEGIN;

DROP TABLE IF EXISTS public.prefix_voc_history;
DROP TABLE IF EXISTS public.prefix_schema_history;

COMMIT;
