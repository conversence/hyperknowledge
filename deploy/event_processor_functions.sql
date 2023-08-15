-- Deploy event_processor_functions
-- requires: event_processor
-- requires: agent_functions
-- idempotent

BEGIN;

\set dbo :dbn '__owner';
\set dbm :dbn '__member';
\set dbc :dbn '__client';

GRANT SELECT, INSERT,UPDATE, DELETE ON TABLE public.event_processor TO :dbm;
-- TODO: Row-level permissions

-- System user with ID 0
INSERT INTO agent (id, email, username, passwd, confirmed, is_admin) VALUES (0, 'system@hyperknowledge.org', 'system', '', true, true)
  ON CONFLICT (id) DO NOTHING;
INSERT INTO event_processor (id, name, owner_id) VALUES (0, 'projections', 0)
  ON CONFLICT (name, owner_id) DO NOTHING;

COMMIT;
