-- Deploy valueobjects
-- requires: topic

BEGIN;

DROP INDEX IF EXISTS idx_struct_entities;
DROP INDEX IF EXISTS idx_langstring;
DROP INDEX IF EXISTS idx_binary_data;
DROP INDEX IF EXISTS idx_struct;

DROP FUNCTION IF EXISTS langtag_as_ltree(varchar);
DROP FUNCTION IF EXISTS langtag_tree_as_iso(ltree);

DROP TABLE IF EXISTS public.schema_defines;
DROP TABLE IF EXISTS public.struct;
DROP TABLE IF EXISTS public.langstring;
DROP TABLE IF EXISTS public.binary_data;

COMMIT;
