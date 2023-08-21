-- Revert artefacts


BEGIN;

DROP TYPE IF EXISTS public.id_type;
DROP TYPE IF EXISTS public.struct_type;
DROP TYPE IF EXISTS public.permission;
DROP SEQUENCE IF EXISTS public.topic_id_seq;


COMMIT;
