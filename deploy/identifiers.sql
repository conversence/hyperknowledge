-- Deploy identifiers
-- requires: topic

BEGIN;

CREATE TABLE IF NOT EXISTS public.vocabulary (
  id BIGINT NOT NULL DEFAULT nextval('public.topic_id_seq'::regclass) PRIMARY KEY,
  uri VARCHAR,
  prefix VARCHAR,
  CONSTRAINT vocabulary_id_fkey FOREIGN KEY (id)
    REFERENCES public.topic (id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX vocabulary_idx ON public.vocabulary (uri varchar_pattern_ops);
CREATE UNIQUE INDEX vocabulary_prefix_idx ON public.vocabulary (prefix varchar_pattern_ops);

CREATE TABLE IF NOT EXISTS public.term (
  id BIGINT NOT NULL DEFAULT nextval('public.topic_id_seq'::regclass) PRIMARY KEY,
  vocabulary_id BIGINT,
  term VARCHAR,
  CONSTRAINT term_id_fkey FOREIGN KEY (id)
    REFERENCES public.topic (id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT term_vocabulary_id_fkey FOREIGN KEY (vocabulary_id)
    REFERENCES public.vocabulary (id)  ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX term_idx ON public.term (vocabulary_id, term varchar_pattern_ops);
-- TODO: How to delete topics when the identity is deleted? Esp. relevant when deleting a namespace!


CREATE TABLE IF NOT EXISTS public.uuidentifier (
  id BIGINT NOT NULL DEFAULT nextval('public.topic_id_seq'::regclass) PRIMARY KEY,
  value UUID DEFAULT uuid_generate_v7(),
  CONSTRAINT uuterm_id_fkey FOREIGN KEY (id)
    REFERENCES public.topic (id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX uuterm_idx ON public.uuidentifier (value);

COMMIT;
