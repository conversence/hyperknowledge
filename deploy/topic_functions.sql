-- Deploy topic_functions
-- requires: identifiers
-- requires: valueobjects
-- idempotent

BEGIN;

\set dbo :dbn '__owner';
\set dbm :dbn '__member';
\set dbc :dbn '__client';

GRANT SELECT,INSERT, UPDATE ON TABLE public.topic TO :dbm;
GRANT SELECT ON TABLE public.topic to :dbc;
GRANT SELECT,INSERT, UPDATE ON TABLE public.vocabulary TO :dbm;
GRANT SELECT ON TABLE public.vocabulary to :dbc;
GRANT SELECT,INSERT ON TABLE public.term TO :dbm;
GRANT SELECT ON TABLE public.term to :dbc;
GRANT SELECT,INSERT ON TABLE public.uuidentifier TO :dbm;
GRANT SELECT ON TABLE public.uuidentifier to :dbc;
GRANT SELECT,INSERT ON TABLE public.langstring TO :dbm;
GRANT SELECT ON TABLE public.langstring to :dbc;
GRANT SELECT,INSERT ON TABLE public.struct TO :dbm;
GRANT SELECT ON TABLE public.struct to :dbc;
GRANT SELECT,INSERT, UPDATE ON TABLE public.binary_data TO :dbm;
GRANT SELECT ON TABLE public.binary_data to :dbc;
GRANT SELECT ON TABLE public.schema_defines TO :dbm;
GRANT SELECT ON TABLE public.schema_defines to :dbc;
GRANT USAGE ON SEQUENCE public.topic_id_seq to :dbm;
GRANT USAGE ON SEQUENCE public.topic_id_seq to :dbc;


CREATE OR REPLACE FUNCTION public.after_create_langstring() RETURNS trigger
  LANGUAGE plpgsql AS $$
    BEGIN
      INSERT INTO public.topic (id, base_type) VALUES (NEW.id, 'langstring');
      RETURN NEW;
    END;
$$;

DROP TRIGGER IF EXISTS after_create_langstring ON public.langstring;
CREATE TRIGGER after_create_langstring AFTER INSERT ON public.langstring FOR EACH ROW EXECUTE FUNCTION public.after_create_langstring();

CREATE OR REPLACE FUNCTION public.ensure_langstring(value_ varchar, lang_ varchar) RETURNS BIGINT
  LANGUAGE plpgsql AS $$
  DECLARE
    ls_id BIGINT;
    inputtag ltree;
  BEGIN
    SELECT public.langtag_as_ltree(lang_) INTO inputtag STRICT;
    -- start with query to avoid hitting the sequence
    SELECT id INTO ls_id FROM public.langstring WHERE value = value_ AND lang=inputtag;
    IF ls_id IS NULL THEN
      -- upsert
      set constraints langstring_id_fkey deferred;
      INSERT INTO public.langstring (value, lang) VALUES (value_, inputtag)
        ON CONFLICT (value, lang) DO NOTHING
        RETURNING id INTO ls_id;
    END IF;
    RETURN ls_id;
  END;
$$;



CREATE OR REPLACE FUNCTION public.after_create_vocabulary() RETURNS trigger
  LANGUAGE plpgsql AS $$
  BEGIN
    INSERT INTO public.topic (id, base_type) VALUES (NEW.id, 'vocabulary');
    RETURN NEW;
  END;
$$;

DROP TRIGGER IF EXISTS after_create_vocabulary ON public.vocabulary;
CREATE TRIGGER after_create_vocabulary AFTER INSERT ON public.vocabulary FOR EACH ROW EXECUTE FUNCTION public.after_create_vocabulary();

CREATE OR REPLACE FUNCTION public.after_create_term() RETURNS trigger
  LANGUAGE plpgsql AS $$
  BEGIN
    INSERT INTO public.topic (id, base_type) VALUES (NEW.id, 'term');
    RETURN NEW;
  END;
$$;

DROP TRIGGER IF EXISTS after_create_term ON public.term;
CREATE TRIGGER after_create_term AFTER INSERT ON public.term FOR EACH ROW EXECUTE FUNCTION public.after_create_term();

CREATE OR REPLACE FUNCTION ensure_vocabulary(vocabulary_ varchar, prefix_ varchar DEFAULT NULL) RETURNS BIGINT
  LANGUAGE plpgsql AS $$
  DECLARE
    voc_id BIGINT;
    voc_prefix varchar;
  BEGIN
    -- start with query to avoid hitting the sequence
    SELECT id, prefix INTO voc_id, voc_prefix FROM public.vocabulary WHERE uri = vocabulary_;
    IF voc_id IS NULL THEN
      -- upsert
      set constraints vocabulary_id_fkey deferred;
      INSERT INTO public.vocabulary (uri, prefix) VALUES (vocabulary_, prefix_)
        ON CONFLICT (uri) DO NOTHING
        RETURNING id INTO voc_id;
    ELSE
      IF prefix_ IS NOT NULL AND (prefix_ != voc_prefix) OR voc_prefix IS NULL THEN
        IF voc_prefix IS NOT NULL THEN
          RAISE WARNING 'Changing a non null prefix';
        END IF;
        UPDATE public.vocabulary SET prefix = prefix_ WHERE id=voc_id;
      END IF;
    END IF;
    RETURN voc_id;
  END;
$$;


CREATE OR REPLACE FUNCTION ensure_term_with_voc(term_ varchar, voc_id BIGINT) RETURNS BIGINT
   LANGUAGE plpgsql AS $$
  DECLARE
    term_id BIGINT;
  BEGIN
    SELECT id INTO term_id FROM public.term idf WHERE vocabulary_id = voc_id AND idf.term = term_;
    IF term_id IS NULL THEN
      -- upsert
      INSERT INTO public.term (vocabulary_id, term) VALUES (voc_id, term_)
        ON CONFLICT (vocabulary_id, term) DO NOTHING
        RETURNING id INTO term_id;
    END IF;
    RETURN term_id;
  END;
$$;


CREATE OR REPLACE FUNCTION ensure_term(term_ varchar, vocabulary varchar DEFAULT NULL, prefix varchar DEFAULT NULL) RETURNS BIGINT
   LANGUAGE plpgsql AS $$
  DECLARE
    voc_id BIGINT;
  BEGIN
    IF vocabulary IS NOT NULL THEN
      SELECT ensure_vocabulary(vocabulary, prefix) INTO voc_id STRICT;
    ELSE
      IF prefix IS NOT NULL THEN
        SELECT id INTO voc_id STRICT FROM vocabulary WHERE vocabulary.prefix = ensure_term.prefix;
      END IF;
    END IF;
    RETURN ensure_term_with_voc(term_, voc_id);
  END;
$$;


CREATE OR REPLACE FUNCTION ensure_term_url(url varchar, prefix varchar DEFAULT NULL) RETURNS BIGINT
  LANGUAGE plpgsql AS $$
  DECLARE
    components text[];
  BEGIN
    SELECT regexp_match(url, '^(\w+:/.*[:/\?#\[\]@])(\w*)$') INTO components;
    IF components IS NULL THEN
      RETURN ensure_term(url);
    ELSE
      RETURN ensure_term(components[1], components[0], prefix);
    END IF;
  END;
$$;

CREATE OR REPLACE FUNCTION get_term_url(url varchar) RETURNS BIGINT
  LANGUAGE plpgsql AS $$
  DECLARE
    components text[];
    id_ BIGINT;
    voc_id BIGINT;
  BEGIN
    SELECT id INTO id_ FROM term WHERE term=url AND vocabulary_id IS NULL;
    IF id_ IS NOT NULL THEN
      RETURN id_;
    END IF;
    SELECT regexp_match(url, '^(\w+:/.*[:/\?#\[\]@])(\w*)$') INTO components;
    IF components IS NOT NULL THEN
      SELECT id INTO voc_id FROM vocabulary WHERE uri=components[0];
      IF voc_id IS NOT NULL  THEN
        SELECT id INTO id_ FROM term WHERE term = components[1] AND vocabulary_id = voc_id;
      END IF;
    END IF;
    RETURN id_;
  END;
$$;



CREATE OR REPLACE FUNCTION public.after_create_uuid() RETURNS trigger
  LANGUAGE plpgsql AS $$
  BEGIN
    INSERT INTO public.topic (id, base_type) VALUES (NEW.id, 'uuid');
    RETURN NEW;
  END;
$$;

DROP TRIGGER IF EXISTS after_create_uuid ON public.uuidentifier;
CREATE TRIGGER after_create_uuid AFTER INSERT ON public.uuidentifier FOR EACH ROW EXECUTE FUNCTION public.after_create_uuid();


CREATE OR REPLACE FUNCTION public.ensure_uuid(value_ UUID DEFAULT NULL) RETURNS BIGINT
  LANGUAGE plpgsql AS $$
  DECLARE
    uid BIGINT;
  BEGIN
    IF value_ IS NOT NULL THEN
      -- start with query to avoid hitting the sequence
      SELECT id INTO uid FROM public.uuidentifier WHERE value=value_;
    END IF;
    IF uid IS NULL THEN
      IF value_ IS NULL THEN
        INSERT INTO public.uuidentifier (value) VALUES (DEFAULT)
        RETURNING id INTO uid;
      ELSE
        INSERT INTO public.uuidentifier (value) VALUES (value_)
        ON CONFLICT (value) DO NOTHING
        RETURNING id INTO uid;
      END IF;
    END IF;
    RETURN uid;
  END;
$$;


CREATE OR REPLACE FUNCTION public.after_create_binary_data() RETURNS trigger
  LANGUAGE plpgsql AS $$
  BEGIN
    INSERT INTO public.topic (id, base_type) VALUES (NEW.id, 'binary_data');
    RETURN NEW;
  END;
$$;

DROP TRIGGER IF EXISTS after_create_binary_data ON public.binary_data;
CREATE TRIGGER after_create_binary_data AFTER INSERT ON public.binary_data FOR EACH ROW EXECUTE FUNCTION public.after_create_binary_data();


CREATE OR REPLACE FUNCTION public.ensure_binary_data(data bytea) RETURNS BIGINT
  LANGUAGE plpgsql AS $$
  DECLARE
    hash_ bytea;
    id_ BIGINT;
  BEGIN
    SELECT sha256(data) INTO hash_ STRICT;
    SELECT id INTO id_ FROM binary_data WHERE hash = hash_;
    IF id_ IS NULL THEN
      -- TODO: Avoid calculating the sha256 twice
      INSERT INTO public.binary_data (value) VALUES (data)
        ON CONFLICT DO NOTHING
        RETURNING id INTO id_;
    END IF;
    return id_;
  END;
$$;


CREATE OR REPLACE FUNCTION public.after_create_struct() RETURNS trigger
  LANGUAGE plpgsql AS $$
  BEGIN
    INSERT INTO public.topic (id, base_type) VALUES (NEW.id, 'struct')
      ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
  END;
$$;

DROP TRIGGER IF EXISTS after_create_struct ON public.struct;
CREATE TRIGGER after_create_struct AFTER INSERT ON public.struct FOR EACH ROW EXECUTE FUNCTION public.after_create_struct();

CREATE OR REPLACE FUNCTION public.ensure_struct(data JSONB, type_ struct_type='other', url VARCHAR=NULL, prefix varchar=NULL, data_schema_url VARCHAR=NULL) RETURNS BIGINT
  LANGUAGE plpgsql AS $$
  DECLARE
    hash_ bytea;
    id_ BIGINT;
    url_id BIGINT = NULL;
    schema_id BIGINT = NULL;
  BEGIN
    SELECT sha256(data::text::bytea) INTO hash_ STRICT;
    SELECT id INTO id_ FROM binary_data WHERE hash = hash_;
    IF url IS NOT NULL THEN
      SELECT ensure_vocabulary(url, prefix) INTO url_id STRICT;
    END IF;
    IF data_schema_url IS NOT NULL THEN
      SELECT get_term_url(data_schema_url) INTO schema_id STRICT;
    END IF;
    IF id_ IS NULL THEN
      INSERT INTO public.struct (value, subtype, is_vocab, data_schema_id) VALUES (data, type_, url_id, schema_id)
        ON CONFLICT (hash, coalesce(data_schema_id, -1)) DO UPDATE SET subtype = type_, data_schema_id = schema_id, is_vocab=url_id
        RETURNING id INTO id_;
    END IF;
    return id_;
  END;
$$;


-- TODO: Add/remove a name to an existing struct


CREATE OR REPLACE FUNCTION extract_terms(sructure JSONB, schema varchar) RETURNS BIGINT[]
  LANGUAGE plpgsql AS $$
  DECLARE
    values BIGINT[] = '{}'::BIGINT[];
    path text;
    value text;
  BEGIN

  END
$$;

COMMIT;
