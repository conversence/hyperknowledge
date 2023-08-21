-- Deploy event_processor_functions


BEGIN;

DROP POLICY IF EXISTS event_processor_update_policy ON public.event_processor;
DROP POLICY IF EXISTS event_processor_delete_policy ON public.event_processor;
DROP POLICY IF EXISTS event_processor_insert_policy ON public.event_processor;
DROP POLICY IF EXISTS event_processor_select_policy ON public.event_processor;
ALTER TABLE public.event_processor DISABLE ROW LEVEL SECURITY;

COMMIT;
