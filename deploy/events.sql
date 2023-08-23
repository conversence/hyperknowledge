-- Deploy events
-- requires: identifiers
-- requires: agent

BEGIN;


CREATE TABLE public.source (
  id BIGINT NOT NULL PRIMARY KEY,
  creator_id BIGINT,
  local_name varchar,
  public_read boolean DEFAULT true,
  public_write boolean DEFAULT false,
  selective_write boolean DEFAULT false,

  CONSTRAINT source_id_fkey FOREIGN KEY (id)
    REFERENCES public.vocabulary (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT source_creator_fkey FOREIGN KEY (creator_id)
    REFERENCES public.agent (id) ON DELETE CASCADE ON UPDATE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS source_local_name_idx on public.source (local_name);

CREATE TABLE source_inclusion (
  included_id BIGINT NOT NULL,
  including_id BIGINT NOT NULL,
  CONSTRAINT source_inclusion_pkey PRIMARY KEY (included_id, including_id),
  CONSTRAINT source_inclusion_included_fkey FOREIGN KEY (included_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT source_inclusion_including_fkey FOREIGN KEY (including_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- index on including?

CREATE TABLE public.event (
  source_id BIGINT NOT NULL,
  created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (statement_timestamp() AT TIME ZONE 'UTC'),
  creator_id BIGINT,
  event_type_id BIGINT NOT NULL,
  data JSONB NOT NULL,
  -- data in-table or struct reference?
  -- in which case valuation here.
  active boolean NOT NULL DEFAULT true,
  CONSTRAINT event_source_time_pkey PRIMARY KEY (source_id, created),
  CONSTRAINT event_creator_fkey FOREIGN KEY (creator_id)
    REFERENCES public.agent (id) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT event_type_fkey FOREIGN KEY (event_type_id)
    REFERENCES public.term(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT event_source_id_fkey FOREIGN KEY (source_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE
);


CREATE TABLE public.last_event (
  source_id BIGINT NOT NULL PRIMARY KEY,
  last_event_ts TIMESTAMP WITHOUT TIME ZONE,
  CONSTRAINT event_source_id_fkey FOREIGN KEY (source_id)
    REFERENCES public.source (id) ON DELETE CASCADE ON UPDATE CASCADE
);

COMMIT;
