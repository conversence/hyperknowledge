-- Deploy topic
-- requires: base
-- Copyright Conversence 2023
-- License: Apache

BEGIN;


CREATE table public.topic (
  id bigint NOT NULL DEFAULT nextval('public.topic_id_seq'::regclass) PRIMARY KEY,
  base_type public.id_type NOT NULL,
  has_projections BIGINT[] NOT NULL DEFAULT '{}'::BIGINT[]
  -- an array of fkeys to the projection tables
);

CREATE INDEX IF NOT EXISTS topic_has_projections_idx ON topic USING gin (has_projections);

COMMIT;
