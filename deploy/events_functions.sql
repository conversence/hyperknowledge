-- Deploy events_functions
-- requires: events
-- requires: permissions
-- requires: agent_functions
-- idempotent

BEGIN;

\set dbo :dbn '__owner';
\set dbm :dbn '__member';
\set dbc :dbn '__client';

GRANT SELECT, INSERT,UPDATE, DELETE ON TABLE public.source TO :dbm;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.event TO :dbm;
GRANT SELECT, INSERT,UPDATE, DELETE ON TABLE public.last_event TO :dbm;  -- TODO: Access control
GRANT SELECT, INSERT,UPDATE, DELETE ON TABLE public.source_inclusion TO :dbm;  -- TODO: Access control
GRANT SELECT ON TABLE public.event TO :dbc;
GRANT SELECT ON TABLE public.source TO :dbc;
GRANT SELECT ON TABLE public.last_event TO :dbc;  -- TODO: Access control
GRANT SELECT ON TABLE public.source_inclusion TO :dbc;  -- TODO: Access control


CREATE OR REPLACE FUNCTION public.ensure_source(
    url varchar, local_name_ varchar, public_read_ boolean DEFAULT true,
    public_write_ boolean DEFAULT false, selective_write_ boolean DEFAULT false,
    includes_source BIGINT[] DEFAULT '{}'::BIGINT[]) RETURNS BIGINT
  LANGUAGE plpgsql AS $$
  DECLARE
    source_url_id BIGINT;
    source_id BIGINT;
    test BOOLEAN;
    included_id BIGINT;
  BEGIN
    SELECT ensure_vocabulary(url, NULL) INTO source_url_id STRICT;
    SELECT id INTO source_id FROM source WHERE id=source_url_id;
    IF source_id IS NULL THEN
      -- check that the included source_ids exist, and that we have read access
      FOREACH source_id IN ARRAY includes_source LOOP
        SELECT can_read_source(source_id) INTO test STRICT FROM source WHERE id=source_id;
        ASSERT test, 'Cannot read source';
      END LOOP;
      INSERT INTO public.source (id, creator_id, local_name, public_read , public_write, selective_write)
        VALUES (source_url_id, current_agent_id(), local_name_, public_read_, public_write_, selective_write_)
      ON CONFLICT (id) DO UPDATE SET local_name = local_name_, public_read = public_read_,
        public_write = public_write_, selective_write = selective_write_, creator_id=current_agent_id();
      UPDATE public.topic SET base_type = 'source' WHERE id=source_url_id;
      FOREACH included_id IN ARRAY includes_source LOOP
        INSERT INTO source_inclusion (including_id, included_id) VALUES (source_url_id, included_id);
      END LOOP;
      -- TODO: ELSE check conformity
    END IF;
    return source_url_id;
  END;
$$;

CREATE OR REPLACE FUNCTION public.included_sources(source_id BIGINT) RETURNS BIGINT[] LANGUAGE sql STABLE AS $$
    with recursive t(id) as (values(source_id) union all select included_id as id from t, source_inclusion where including_id = t.id) select array_agg(id) from t;
$$;

CREATE OR REPLACE FUNCTION public.including_sources(source_id BIGINT) RETURNS BIGINT[] LANGUAGE sql STABLE AS $$
    with recursive t(id) as (values(source_id) union all select including_id as id from t, source_inclusion where included_id = t.id) select array_agg(id) from t;
$$;

CREATE OR REPLACE FUNCTION public.after_create_source() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE curuser varchar;
    DECLARE agent_id BIGINT;
    BEGIN
        INSERT INTO public.last_event (source_id) VALUES (NEW.id);
        SELECT current_agent_id() INTO agent_id STRICT;
        curuser := current_user;
        EXECUTE 'SET LOCAL ROLE ' || current_database() || '__owner';
        INSERT INTO public.agent_source_permission (source_id, agent_id, is_admin, allow_read, allow_write, allow_all_write) VALUES (NEW.id, agent_id, true, true, true, true);
        EXECUTE 'SET LOCAL ROLE ' || curuser;
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

CREATE OR REPLACE VIEW agent_source_my_permissions AS
    SELECT source_id, is_admin, allow_read, allow_write, allow_all_write
    FROM public.agent_source_permission WHERE agent_id=current_agent_id() AND NOT is_request;

CREATE OR REPLACE FUNCTION can_read_source(source_id BIGINT) RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
    SELECT (SELECT public_read OR creator_id = current_agent_id() FROM public.source s WHERE s.id = source_id) OR
    is_superadmin() OR
    (SELECT allow_read FROM public.agent_source_my_permissions p WHERE p.source_id = source_id);
$$;

GRANT SELECT ON agent_source_my_permissions TO :dbm;
GRANT SELECT ON agent_source_my_permissions TO :dbc;

CREATE OR REPLACE VIEW agent_source_my_selective_permissions AS
    SELECT source_id, event_type_id
    FROM public.agent_source_selective_permission WHERE agent_id=current_agent_id() AND NOT is_request;

GRANT SELECT ON agent_source_my_selective_permissions TO :dbm;
GRANT SELECT ON agent_source_my_selective_permissions TO :dbc;


ALTER TABLE public.source ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS source_select_policy ON public.source;
CREATE POLICY source_select_policy ON public.source FOR SELECT USING (source.public_read OR creator_id = current_agent_id() OR is_superadmin() OR
    (SELECT allow_read FROM agent_source_my_permissions WHERE source_id = source.id));


DROP POLICY IF EXISTS source_delete_policy ON public.source;
CREATE POLICY source_delete_policy ON public.source FOR DELETE USING (
    creator_id = current_agent_id() OR is_superadmin() OR
    (SELECT is_admin FROM agent_source_my_permissions WHERE source_id = source.id));
-- TODO: do not delete a source if others depend on it?

DROP POLICY IF EXISTS source_update_policy ON public.source;
CREATE POLICY source_update_policy ON public.source FOR UPDATE USING (
    creator_id = current_agent_id() OR is_superadmin() OR
    (SELECT is_admin FROM agent_source_my_permissions WHERE source_id = source.id));

DROP POLICY IF EXISTS source_insert_policy ON public.source;
CREATE POLICY source_insert_policy ON public.source FOR INSERT WITH CHECK (true);

ALTER TABLE public.event ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS event_delete_policy ON public.event;
CREATE POLICY event_delete_policy ON public.event FOR DELETE USING (is_superadmin());
-- TODO: Allow special case of dropping a source you own (and that nobody else depends on...) cascading to events
-- maybe add a "deleting" flag on source?

DROP POLICY IF EXISTS event_update_policy ON public.event;
CREATE POLICY event_update_policy ON public.event FOR UPDATE USING (is_superadmin());

DROP POLICY IF EXISTS event_insert_policy ON public.event;
CREATE POLICY event_insert_policy ON public.event FOR INSERT WITH CHECK (
    (SELECT allow_all_write FROM public.agent_source_my_permissions WHERE source_id = event.source_id) OR
    (SELECT public_write AND NOT selective_write FROM public.source s WHERE s.id = source_id) OR
    is_superadmin() OR
    (SELECT count(*) FROM public.agent_source_my_selective_permissions p WHERE p.source_id = event.source_id AND p.event_type_id = event.event_type_id) > 0
);

DROP POLICY IF EXISTS event_select_policy ON public.event;
CREATE POLICY event_select_policy ON public.event FOR SELECT USING (can_read_source(source_id));

-- TODO next: Apply can_read_source to projections...

-- TODO: restrict access to the agent_source_permission table. Allow source is_admin to edit that.


ALTER TABLE public.agent_source_permission ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agent_source_permission_delete_policy ON public.agent_source_permission;
CREATE POLICY agent_source_permission_delete_policy ON public.agent_source_permission FOR DELETE USING (
    agent_id = current_agent_id() OR is_superadmin() OR (SELECT is_admin FROM agent_source_my_permissions p WHERE p.source_id = agent_source_permission.source_id));

DROP POLICY IF EXISTS agent_source_permission_update_policy ON public.agent_source_permission;
CREATE POLICY agent_source_permission_update_policy ON public.agent_source_permission FOR UPDATE USING (
    is_superadmin() OR (SELECT is_admin FROM agent_source_my_permissions p WHERE p.source_id = agent_source_permission.source_id) OR (agent_id = current_agent_id() AND is_request= TRUE));

DROP POLICY IF EXISTS agent_source_permission_insert_policy ON public.agent_source_permission;
CREATE POLICY agent_source_permission_insert_policy ON public.agent_source_permission FOR INSERT WITH CHECK (
    is_superadmin() OR (SELECT is_admin FROM agent_source_my_permissions p WHERE p.source_id = agent_source_permission.source_id) OR (agent_id = current_agent_id() AND is_request=TRUE));
-- Bootstrap problem, requires permission elevation in after_create_source

DROP POLICY IF EXISTS agent_source_permission_select_policy ON public.agent_source_permission;
CREATE POLICY agent_source_permission_select_policy ON public.agent_source_permission FOR SELECT USING (
    agent_id = current_agent_id() OR is_superadmin() OR
    (SELECT is_admin FROM agent_source_my_permissions p WHERE p.source_id = agent_source_permission.source_id));

ALTER TABLE public.agent_source_selective_permission ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS agent_source_selective_permission_delete_policy ON public.agent_source_selective_permission;
CREATE POLICY agent_source_selective_permission_delete_policy ON public.agent_source_selective_permission FOR DELETE USING (
    agent_id = current_agent_id() OR is_superadmin() OR (SELECT is_admin FROM agent_source_my_permissions p WHERE p.source_id = agent_source_selective_permission.source_id));

DROP POLICY IF EXISTS agent_source_selective_permission_update_policy ON public.agent_source_selective_permission;
CREATE POLICY agent_source_selective_permission_update_policy ON public.agent_source_selective_permission FOR UPDATE USING (
    is_superadmin() OR (SELECT is_admin FROM agent_source_my_permissions p WHERE p.source_id = agent_source_selective_permission.source_id) OR (agent_id = current_agent_id() AND is_request= TRUE));

DROP POLICY IF EXISTS agent_source_selective_permission_insert_policy ON public.agent_source_selective_permission;
CREATE POLICY agent_source_selective_permission_insert_policy ON public.agent_source_selective_permission FOR INSERT WITH CHECK (
    is_superadmin() OR (SELECT is_admin FROM agent_source_my_permissions p WHERE p.source_id = agent_source_selective_permission.source_id) OR (agent_id = current_agent_id() AND is_request= TRUE));

DROP POLICY IF EXISTS agent_source_selective_permission_select_policy ON public.agent_source_selective_permission;
CREATE POLICY agent_source_selective_permission_select_policy ON public.agent_source_selective_permission FOR SELECT USING (
    agent_id = current_agent_id() OR is_superadmin() OR
    (SELECT is_admin FROM agent_source_my_permissions p WHERE p.source_id = agent_source_selective_permission.source_id));

-- TODO next: Apply can_read_source to projections...



COMMIT;
