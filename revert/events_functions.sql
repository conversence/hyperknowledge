-- Deploy events_functions


BEGIN;

DROP TRIGGER IF EXISTS after_create_event ON public.event;
DROP TRIGGER IF EXISTS after_create_source ON public.source;
DROP FUNCTION IF EXISTS public.after_create_event();
DROP FUNCTION IF EXISTS public.after_create_source();


COMMIT;
