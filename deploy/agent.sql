-- Deploy agent
-- requires: base
-- Copyright Society Library and Conversence 2022-2023

BEGIN;

CREATE TABLE IF NOT EXISTS public.agent (
    id bigint NOT NULL DEFAULT nextval('public.topic_id_seq'::regclass),
    email character varying(255) NOT NULL,
    username character varying(255) NOT NULL,
    passwd character varying(255) NOT NULL,
    confirmed boolean DEFAULT false,
    is_admin boolean DEFAULT false,
    created timestamp without time zone NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    last_login_email_sent timestamp without time zone,
    CONSTRAINT agent_pkey PRIMARY KEY (id),
    CONSTRAINT agent_username_key UNIQUE (username),
    CONSTRAINT agent_email_key UNIQUE (email)
);


CREATE OR REPLACE VIEW public.public_agent (id, username) AS
SELECT
    id,
    username FROM public.agent;

COMMIT;
