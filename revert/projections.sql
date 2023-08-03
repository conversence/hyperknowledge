-- Deploy projections


BEGIN;

DELETE FROM projection_table;  -- should delete all projection tables

DROP TRIGGER IF EXISTS before_create_projection_table ON public.projection_table;
DROP TRIGGER IF EXISTS after_delete_projection_table ON public.projection_table;

DROP FUNCTION IF EXISTS before_create_projection_table();
DROP FUNCTION IF EXISTS after_delete_projection_table();

DROP TABLE IF EXISTS projection_table;

COMMIT;
