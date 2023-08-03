-- Revert topics_functions.sql:


BEGIN;

DROP TRIGGER IF EXISTS after_create_langstring ON public.langstring;
DROP TRIGGER IF EXISTS after_create_vocabulary ON public.vocabulary;
DROP TRIGGER IF EXISTS after_create_term ON public.term;
DROP TRIGGER IF EXISTS after_create_uuid ON public.uuidentifier;
DROP TRIGGER IF EXISTS after_create_binary_data ON public.binary_data;
DROP TRIGGER IF EXISTS after_create_struct ON public.struct;

DROP FUNCTION IF EXISTS public.after_create_langstring();
DROP FUNCTION IF EXISTS public.ensure_langstring(varchar, varchar);
DROP FUNCTION IF EXISTS public.after_create_vocabulary();
DROP FUNCTION IF EXISTS public.after_create_term();
DROP FUNCTION IF EXISTS ensure_vocabulary(varchar, varchar);
DROP FUNCTION IF EXISTS ensure_term(varchar, varchar, varchar);
DROP FUNCTION IF EXISTS ensure_term_with_voc(varchar, BIGINT);
DROP FUNCTION IF EXISTS public.after_create_uuid();
DROP FUNCTION IF EXISTS public.ensure_uuid(UUID);
DROP FUNCTION IF EXISTS public.ensure_binary_data(bytea);
DROP FUNCTION IF EXISTS public.ensure_source(varchar, varchar, boolean);
DROP FUNCTION IF EXISTS public.ensure_struct(JSONB, struct_type, varchar, varchar, varchar);
DROP FUNCTION IF EXISTS public.after_create_binary_data();
DROP FUNCTION IF EXISTS public.after_create_struct();
DROP FUNCTION IF EXISTS ensure_term_url(varchar, varchar);
DROP FUNCTION IF EXISTS get_term_url(varchar);
DROP FUNCTION IF EXISTS extract_terms(JSONB, varchar);

COMMIT;
