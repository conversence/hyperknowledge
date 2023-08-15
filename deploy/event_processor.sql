-- Deploy event_processor
-- requires: events

BEGIN;

CREATE TABLE public.event_processor (
  id serial PRIMARY KEY,
  name varchar NOT NULL,
  owner_id BIGINT NOT NULL,
  source_id BIGINT,
  last_event_ts TIMESTAMP WITHOUT TIME ZONE,
  CONSTRAINT event_processor_source_id_fkey FOREIGN KEY (source_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT event_processor_owner_id_fkey FOREIGN KEY (owner_id)
    REFERENCES public.agent (id) ON DELETE CASCADE ON UPDATE CASCADE
);


CREATE UNIQUE INDEX IF NOT EXISTS event_processor_name_idx ON public.event_processor (name, owner_id);


COMMIT;
