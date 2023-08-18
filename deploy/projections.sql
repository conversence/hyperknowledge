-- Deploy projections
-- requires: events
-- requires: valueobjects
-- requires: events_functions

BEGIN;

CREATE TABLE IF NOT EXISTS public.projection_table (
  schema_id BIGINT NOT NULL,
  tablename varchar NOT NULL,

  CONSTRAINT projection_table_pkey PRIMARY KEY (schema_id, tablename),
  CONSTRAINT projection_table_schema_id FOREIGN KEY (schema_id)
    REFERENCES public.struct (id) ON DELETE CASCADE ON UPDATE CASCADE
  -- Note: No fkey to system tables, much less views, so no fkey for tablename
);

-- put the triggers in this file so revert can use them

CREATE OR REPLACE FUNCTION public.before_create_projection_table() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  tname varchar;
BEGIN
  -- TODO: Also check on update.
  SELECT tablename INTO tname STRICT FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = 'NEW.tablename';
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS before_create_projection_table ON public.projection_table;
CREATE TRIGGER before_create_projection_table BEFORE INSERT ON public.projection_table FOR EACH ROW EXECUTE FUNCTION public.before_create_projection_table();

CREATE OR REPLACE FUNCTION public.after_delete_projection_table() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE 'DROP TABLE IF EXISTS public."' || OLD.tablename || '" CASCADE;';
  RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS after_delete_projection_table ON public.projection_table;
CREATE TRIGGER after_delete_projection_table AFTER DELETE ON public.projection_table FOR EACH ROW EXECUTE FUNCTION public.after_delete_projection_table();

-- TODO: a sql_drop event trigger so I can delete the row if the table gets deleted

COMMIT;
