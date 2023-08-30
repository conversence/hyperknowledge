-- Deploy context_history
-- requires: identifiers
-- requires: valueobjects

BEGIN;

CREATE TABLE public.prefix_voc_history (
  prefix VARCHAR NOT NULL,
  added TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (statement_timestamp() AT TIME ZONE 'UTC'),
  voc_id BIGINT NOT NULL,

  CONSTRAINT prefix_voc_history_pkey PRIMARY KEY (prefix, added),

  CONSTRAINT prefix_voc_history_id_fkey FOREIGN KEY (voc_id)
    REFERENCES public.vocabulary (id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS prefix_voc_history_voc_id_idx ON public.prefix_voc_history USING btree (voc_id);
-- unique for non-superseded... probably not worth it.
CREATE INDEX IF NOT EXISTS prefix_voc_history_added_idx ON public.prefix_voc_history USING brin (added);

CREATE TABLE public.prefix_schema_history (
  prefix VARCHAR NOT NULL,
  added TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (statement_timestamp() AT TIME ZONE 'UTC'),
  schema_id BIGINT NOT NULL,

  CONSTRAINT prefix_schema_history_pkey PRIMARY KEY (prefix, added),

  CONSTRAINT prefix_schema_history_id_fkey FOREIGN KEY (schema_id)
    REFERENCES public.struct (id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS prefix_schema_history_schema_id_idx ON public.prefix_schema_history USING btree (schema_id);
CREATE INDEX IF NOT EXISTS prefix_schema_history_added_idx ON public.prefix_schema_history USING brin (added);

COMMIT;
