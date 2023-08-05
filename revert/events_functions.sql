-- Deploy events_functions


BEGIN;

DROP FUNCTION IF EXISTS public.ensure_source(varchar, varchar, boolean);

DROP POLICY IF EXISTS event_delete_policy ON public.event;
DROP POLICY IF EXISTS event_update_policy ON public.event;
DROP POLICY IF EXISTS event_insert_policy ON public.event;
DROP POLICY IF EXISTS event_select_policy ON public.event;

ALTER TABLE public.event DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS source_delete_policy ON public.source;
DROP POLICY IF EXISTS source_update_policy ON public.source;
DROP POLICY IF EXISTS source_insert_policy ON public.source;
DROP POLICY IF EXISTS source_select_policy ON public.source;
ALTER TABLE public.source DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agent_source_permission_delete_policy ON public.agent_source_permission;
DROP POLICY IF EXISTS agent_source_permission_update_policy ON public.agent_source_permission;
DROP POLICY IF EXISTS agent_source_permission_insert_policy ON public.agent_source_permission;
DROP POLICY IF EXISTS agent_source_permission_select_policy ON public.agent_source_permission;
ALTER TABLE public.agent_source_permission DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agent_source_selective_permission_delete_policy ON public.agent_source_selective_permission;
DROP POLICY IF EXISTS agent_source_selective_permission_update_policy ON public.agent_source_selective_permission;
DROP POLICY IF EXISTS agent_source_selective_permission_insert_policy ON public.agent_source_selective_permission;
DROP POLICY IF EXISTS agent_source_selective_permission_select_policy ON public.agent_source_selective_permission;
ALTER TABLE public.agent_source_selective_permission DISABLE ROW LEVEL SECURITY;

DROP VIEW IF EXISTS agent_source_my_permissions;
DROP VIEW IF EXISTS agent_source_my_selective_permissions;

DROP FUNCTION IF EXISTS can_read_source(bigint);

DROP TRIGGER IF EXISTS after_create_event ON public.event;
DROP TRIGGER IF EXISTS after_create_source ON public.source;
DROP FUNCTION IF EXISTS public.after_create_event();
DROP FUNCTION IF EXISTS public.after_create_source();

COMMIT;
