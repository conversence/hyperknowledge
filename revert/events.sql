-- Deploy events

BEGIN;

DROP TABLE IF EXISTS public.last_event;
DROP TABLE IF EXISTS public.event;
DROP TABLE IF EXISTS public.source_inclusion;
-- Will cascade to projections
DROP TABLE IF EXISTS public.source CASCADE;

COMMIT;
