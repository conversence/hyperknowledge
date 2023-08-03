-- Deploy events_functions
-- requires: events
-- requires: permissions
-- idempotent

BEGIN;

-- TODO : row-level permissions on sources and events

CREATE OR REPLACE FUNCTION public.after_create_source() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
    INSERT INTO public.last_event (source_id) VALUES (NEW.id);
    RETURN NEW;
    END;
    $$;

DROP TRIGGER IF EXISTS after_create_source ON public.source;
CREATE TRIGGER after_create_source AFTER INSERT ON public.source FOR EACH ROW EXECUTE FUNCTION public.after_create_source();


CREATE OR REPLACE FUNCTION public.after_create_event() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
    UPDATE public.last_event SET last_event_ts=CASE WHEN last_event_ts > NEW.created THEN last_event_ts ELSE NEW.created END WHERE source_id = NEW.source_id;
    EXECUTE 'NOTIFY events,' || quote_literal(NEW.created);
    RETURN NEW;
    END;
    $$;

DROP TRIGGER IF EXISTS after_create_event ON public.event;
CREATE TRIGGER after_create_event AFTER INSERT ON public.event FOR EACH ROW EXECUTE FUNCTION public.after_create_event();

COMMIT;
