-- Deploy event_processor_functions
-- requires: event_processor
-- requires: agent_functions
-- idempotent

BEGIN;

-- System user with ID 0
INSERT INTO agent (id, email, username, passwd, confirmed, is_admin) VALUES (0, 'system@hyperknowledge.org', 'system', '', true, true)
  ON CONFLICT (id) DO NOTHING;
INSERT INTO event_processor (name, owner_id, all_sources) VALUES ('processor', 0, true)
  ON CONFLICT (name, owner_id) DO NOTHING;
INSERT INTO event_processor_global_status (id) (SELECT id FROM event_processor WHERE name='processor' AND owner_id=0)
  ON CONFLICT (id) DO NOTHING;

COMMIT;
