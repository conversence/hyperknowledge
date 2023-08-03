-- Deploy events

BEGIN;

DROP VIEW IF EXISTS public.public_agent;
DROP TABLE IF EXISTS public.agent;

COMMIT;
