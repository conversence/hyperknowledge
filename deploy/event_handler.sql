-- Deploy event_handler
-- requires: events

BEGIN;

CREATE TYPE event_handler_language AS ENUM (
  'wasm',
  'javascript',
  'python'
);

CREATE TABLE IF NOT EXISTS public.event_handler (
  id int primary key GENERATED ALWAYS AS IDENTITY,
  event_type_id BIGINT NOT NULL,
  target_role varchar NOT NULL,
  target_range_id BIGINT NOT NULL,
  language event_handler_language NOT NULL,
  code_text TEXT,
  code_binary bytea,

  CONSTRAINT event_handler_event_type_fkey FOREIGN KEY (event_type_id)
    REFERENCES public.term (id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT event_handler_target_range_fkey FOREIGN KEY (target_range_id)
    REFERENCES public.term (id) ON DELETE CASCADE ON UPDATE CASCADE
);

DROP INDEX IF EXISTS event_handler_unique_idx;
CREATE UNIQUE INDEX IF NOT EXISTS event_handler_unique_idx ON public.event_handler (event_type_id, target_role, target_range_id);

COMMIT;
