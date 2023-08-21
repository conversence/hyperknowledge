-- Deploy event_processor_functions
-- requires: event_processor
-- requires: agent_functions
-- idempotent

BEGIN;

\set dbo :dbn '__owner';
\set dbm :dbn '__member';
\set dbc :dbn '__client';

GRANT SELECT, INSERT,UPDATE, DELETE ON TABLE public.event_processor TO :dbm;

ALTER TABLE public.event_processor ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS event_processor_update_policy ON public.event_processor;
CREATE POLICY event_processor_update_policy ON public.event_processor FOR UPDATE USING (owner_id = current_agent_id() OR public.is_superadmin());
DROP POLICY IF EXISTS event_processor_delete_policy ON public.event_processor;
CREATE POLICY event_processor_delete_policy ON public.event_processor FOR DELETE USING (owner_id = current_agent_id() OR public.is_superadmin());
DROP POLICY IF EXISTS event_processor_insert_policy ON public.event_processor;
CREATE POLICY event_processor_insert_policy ON public.event_processor FOR INSERT WITH CHECK (owner_id = current_agent_id());
DROP POLICY IF EXISTS event_processor_select_policy ON public.event_processor;
CREATE POLICY event_processor_select_policy ON public.event_processor FOR SELECT USING (owner_id = current_agent_id() OR public.is_superadmin());


-- System user with ID 0
INSERT INTO agent (id, email, username, passwd, confirmed, permissions) VALUES (0, 'system@hyperknowledge.org', 'system', '', true, '{"admin"}')
  ON CONFLICT (id) DO NOTHING;
INSERT INTO event_processor (id, name, owner_id) VALUES (0, 'projections', 0)
  ON CONFLICT (name, owner_id) DO NOTHING;

COMMIT;
