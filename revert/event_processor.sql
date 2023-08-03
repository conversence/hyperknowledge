-- Deploy event_processor


BEGIN;

DROP TABLE IF EXISTS public.event_processor_global_status;
DROP TABLE IF EXISTS public.event_processor_source_status;
DROP TABLE IF EXISTS public.event_processor;

COMMIT;
