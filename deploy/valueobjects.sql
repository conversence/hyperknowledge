-- Deploy valueobjects
-- requires: identifiers

BEGIN;


CREATE OR REPLACE FUNCTION langtag_as_ltree(tag varchar) RETURNS ltree LANGUAGE sql STABLE AS $$
  SELECT text2ltree(concat_ws('.', VARIADIC array_agg(trim(both '{}' from CAST(r AS varchar))))) from (
    SELECT regexp_matches(tag,'[a-z][_a-z0-9][a-z0-9]*','ig') r) AS rm;
$$;

CREATE OR REPLACE FUNCTION langtag_tree_as_iso(tag ltree) RETURNS varchar LANGUAGE sql STABLE AS $$
  SELECT concat_ws('_',variadic string_to_array(cast(tag as varchar), '.'));
$$;


CREATE TABLE IF NOT EXISTS public.langstring (
  id BIGINT NOT NULL DEFAULT nextval('public.topic_id_seq'::regclass) PRIMARY KEY,
  value TEXT NOT NULL,
  lang ltree NOT NULL,
  CONSTRAINT langstring_id_fkey FOREIGN KEY (id)
    REFERENCES public.topic (id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED
);
-- Translation data would go elsewhere
-- and why not ipld here?

DROP INDEX IF EXISTS idx_langstring;
CREATE UNIQUE INDEX idx_langstring ON langstring (value, lang);


CREATE TABLE IF NOT EXISTS public.binary_data (
  id BIGINT NOT NULL DEFAULT nextval('public.topic_id_seq'::regclass) PRIMARY KEY,
  value bytea NOT NULL,
  hash bytea GENERATED ALWAYS AS (sha256(value)) STORED,
  -- replace with IPFS?
  CONSTRAINT binary_data_id_fkey FOREIGN KEY (id)
    REFERENCES public.topic (id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED
);

DROP INDEX IF EXISTS idx_binary_data;
CREATE UNIQUE INDEX idx_binary_data ON public.binary_data (hash);

CREATE TABLE IF NOT EXISTS public.struct (
  id BIGINT NOT NULL DEFAULT nextval('public.topic_id_seq'::regclass) PRIMARY KEY,
  value jsonb NOT NULL,
  data_schema_id BIGINT REFERENCES public.topic(id),
  -- this will usually be a term reference, probably coming from a struct of subtype data_schema
  hash bytea GENERATED ALWAYS AS (sha256(value::text::bytea)) STORED,
  -- Replace with IPLD/CBOR?
  subtype struct_type NOT NULL DEFAULT 'other',
  is_vocab BIGINT, -- IDEALLY I should just share the ID with vocabulary... but I'd need multiple inheritance in sqlalchemy
  entities BIGINT[],
  CONSTRAINT struct_id_fkey FOREIGN KEY (id)
    REFERENCES public.topic (id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED,
  CONSTRAINT struct_is_vocab_fkey FOREIGN KEY (is_vocab)
    REFERENCES public.vocabulary (id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED
);

DROP INDEX IF EXISTS idx_struct;
CREATE UNIQUE INDEX idx_struct ON public.struct (hash, coalesce(data_schema_id, -1));

DROP INDEX IF EXISTS idx_struct_entities;
CREATE INDEX idx_struct_entities ON public.struct USING gin (entities);

-- TODO: Add ondelete to all of these so the topic dies.

CREATE TABLE IF NOT EXISTS public.schema_defines (
  term_id BIGINT PRIMARY KEY,
  schema_id BIGINT NOT NULL,
  CONSTRAINT schema_defines_term_fkey FOREIGN KEY (term_id)
    REFERENCES public.term(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT schema_defines_schema_fkey FOREIGN KEY (schema_id)
    REFERENCES public.struct(id) ON DELETE CASCADE ON UPDATE CASCADE
);

COMMIT;
