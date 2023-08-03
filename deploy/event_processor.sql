-- Deploy event_processor
-- requires: events

BEGIN;

CREATE TABLE public.event_processor (
  id serial PRIMARY KEY,
  name varchar NOT NULL,
  owner_id BIGINT NOT NULL,
  all_sources boolean DEFAULT false,
  CONSTRAINT event_processor_owner_id_fkey FOREIGN KEY (owner_id)
    REFERENCES public.agent (id) ON DELETE CASCADE ON UPDATE CASCADE
);


CREATE UNIQUE INDEX IF NOT EXISTS event_processor_name_idx ON public.event_processor (name, owner_id);


CREATE TABLE public.event_processor_source_status (
  id INTEGER NOT NULL,
  source_id BIGINT,
  last_event_ts TIMESTAMP WITHOUT TIME ZONE,
  CONSTRAINT event_processor_source_status_pkey PRIMARY KEY (id, source_id),
  CONSTRAINT event_processor_source_status_id_fkey FOREIGN KEY (id)
    REFERENCES public.event_processor (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT event_processor_source_status_source_id_fkey FOREIGN KEY (source_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE public.event_processor_global_status (
  id INTEGER NOT NULL,
  last_event_ts TIMESTAMP WITHOUT TIME ZONE,
  CONSTRAINT event_processor_global_status_pkey PRIMARY KEY (id),
  CONSTRAINT event_processor_global_status_id_fkey FOREIGN KEY (id)
    REFERENCES public.event_processor (id) ON DELETE CASCADE ON UPDATE CASCADE
);


COMMIT;
