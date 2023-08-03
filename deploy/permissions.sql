-- Deploy permissions
-- requires: events

BEGIN;

CREATE TABLE agent_source_permission (
  agent_id BIGINT NOT NULL,
  source_id BIGINT NOT NULL,
  allow_write boolean DEFAULT false,

  CONSTRAINT agent_source_permission_pkey PRIMARY KEY (agent_id, source_id),
  CONSTRAINT agent_source_permission_agent_fkey FOREIGN KEY (agent_id)
    REFERENCES public.agent (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT agent_source_permission_source_fkey FOREIGN KEY (agent_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- TODO: Some commands need special write permissions, have another table for that case
-- TODO: restrict access to the agent_source_permission table.
-- TODO: aggregate read permissions?

COMMIT;
