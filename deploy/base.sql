-- requires: admin

BEGIN;

CREATE SEQUENCE IF NOT EXISTS public.topic_id_seq
    AS BIGINT
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TYPE public.permission AS ENUM (
    'add_schema',
    'add_source',
    'add_handler',
    'admin'
);

CREATE TYPE public.id_type AS ENUM (
    -- references
    'vocabulary',
    'term',
    'uuid',
    'concepts',
    -- special cases
    'source',  -- the reference is a url
    -- literals and value objects
    'langstring',
    'binary_data',
    'struct'
    -- 'document',
);

CREATE TYPE public.struct_type AS ENUM (
    'hk_schema',
    'hk_schema_element',
    'ontology',
    'ld_context',
    'other'
);

COMMIT;
