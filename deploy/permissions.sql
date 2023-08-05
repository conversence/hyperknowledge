-- Deploy permissions
-- requires: events

BEGIN;

CREATE TABLE IF NOT EXISTS agent_source_permission (
  agent_id BIGINT NOT NULL,
  source_id BIGINT NOT NULL,
  is_admin boolean DEFAULT false,
  allow_read boolean DEFAULT false,
  allow_write boolean DEFAULT false,
  allow_all_write boolean DEFAULT false,

  CONSTRAINT agent_source_permission_pkey PRIMARY KEY (agent_id, source_id),
  CONSTRAINT agent_source_permission_agent_fkey FOREIGN KEY (agent_id)
    REFERENCES public.agent (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT agent_source_permission_source_fkey FOREIGN KEY (source_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE IF NOT EXISTS agent_source_selective_permission (
  agent_id BIGINT NOT NULL,
  source_id BIGINT NOT NULL,
  event_type_id BIGINT NOT NULL,

  CONSTRAINT agent_source_selective_permission_pkey PRIMARY KEY (agent_id, source_id, event_type_id),
  CONSTRAINT agent_source_selective_permission_agent_fkey FOREIGN KEY (agent_id)
    REFERENCES public.agent (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT agent_source_selective_permission_source_fkey FOREIGN KEY (source_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT agent_source_selective_permission_event_type_fkey FOREIGN KEY (event_type_id)
    REFERENCES public.term (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- TODO: aggregate read permissions?

COMMIT;
