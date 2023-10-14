-- Deploy sensecraft:agent_functions to pg
-- requires: agent
-- idempotent

BEGIN;

\set dbo :dbn '__owner';
\set dbm :dbn '__member';
\set dbc :dbn '__client';


--
-- Name: TABLE agent; Type: ACL
--

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.agent TO :dbm;
GRANT SELECT, INSERT ON TABLE public.agent TO :dbc;

-- REVOKE SELECT ON TABLE public.agent FROM :dbc;
-- GRANT SELECT (id, username, username) ON TABLE public.agent TO :dbc;
GRANT SELECT ON public.public_agent TO :dbc;
GRANT SELECT ON public.public_agent TO :dbm;


--
-- Name: role_to_id(character varying); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.role_to_id(role character varying) RETURNS integer
AS $$
  SELECT CASE
    role ~ ('^' || current_database() || '__[mglq]_\d+')
    WHEN true THEN
      substr(role, char_length(current_database())+5)::integer
    ELSE
      NULL
    END;
$$ LANGUAGE SQL IMMUTABLE;


--
-- Name: current_agent_id(); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.current_agent_id() RETURNS integer
AS $$
  SELECT role_to_id(cast(current_user as varchar));
$$ LANGUAGE SQL STABLE;


--
-- Name: current_agent(); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.current_agent() RETURNS public.agent
AS $$
  SELECT * from public.agent WHERE id = public.current_agent_id()
$$ LANGUAGE SQL STABLE;

--
-- Name: is_superadmin(); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.is_superadmin() RETURNS boolean
AS $$
  SELECT (current_user = current_database()||'__owner') OR count(*) > 0
    FROM pg_catalog.pg_roles r JOIN pg_catalog.pg_auth_members m
    ON (m.member = r.oid)
    JOIN pg_roles r1 ON (m.roleid=r1.oid)
    WHERE r1.rolname = current_database()||'__admin'
    AND r.rolname=current_user AND r.rolinherit;
$$ LANGUAGE SQL STABLE;


--
-- Name: has_permission(character varying); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.has_permission(permission public.permission) RETURNS boolean
AS $$
  SELECT public.is_superadmin() OR (current_agent_id() IS NOT NULL AND (
    SELECT permission = ANY(permissions) FROM agent where id=current_agent_id()));
$$ LANGUAGE SQL STABLE;


--
-- Name: get_token(character varying, character varying); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.get_token(username_ character varying, pass character varying, duration integer=1000) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
    DECLARE agent_id BIGINT;
    DECLARE passh varchar;
    DECLARE curuser varchar;
    DECLARE is_confirmed boolean;
    BEGIN
      curuser := current_user;
      EXECUTE 'SET LOCAL ROLE ' || current_database() || '__rolemaster';
      SELECT id, passwd, confirmed INTO STRICT agent_id, passh, is_confirmed FROM agent WHERE username=username_;
      IF NOT is_confirmed THEN
        RAISE EXCEPTION 'invalid confirmed / Cannot login until confirmed';
      END IF;
      IF passh = crypt(pass, passh) THEN
        SELECT sign(row_to_json(r), current_setting('app.jwt_secret')) INTO STRICT passh FROM (
          SELECT CONCAT('agent:', agent_id::varchar) AS sub, extract(epoch from now())::integer + duration AS exp) r;
        UPDATE agent SET last_login = (now() AT TIME ZONE 'UTC') WHERE username=username_;
        EXECUTE 'SET LOCAL ROLE ' || curuser;
        RETURN passh;
      ELSE
        EXECUTE 'SET LOCAL ROLE ' || curuser;
        RETURN NULL;
      END IF;
    END;
$$;


--
-- Name: renew_token(character varying); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.renew_token(token character varying, duration integer=1000) RETURNS character varying
    LANGUAGE plpgsql
    AS $$
    DECLARE p json;
    DECLARE t varchar;
    DECLARE v boolean;
    DECLARE curuser varchar;
    DECLARE agent_id BIGINT;
    BEGIN
      SELECT payload, valid INTO STRICT p, v FROM verify(token, current_setting('app.jwt_secret'));
      IF NOT v THEN
        RETURN NULL;
      END IF;
      IF (p ->> 'exp')::integer < extract(epoch from now())::integer THEN
        RETURN NULL;
      END IF;
      SELECT cast(substr(p ->> 'sub', 7) as BIGINT) INTO STRICT agent_id;
      IF agent_id != (SELECT id FROM agent WHERE id = agent_id) THEN
        RETURN NULL;
      END IF;
      SELECT sign(row_to_json(r), current_setting('app.jwt_secret')) INTO STRICT t FROM (
        SELECT (p ->> 'sub') as sub, extract(epoch from now())::integer + duration AS exp) r;
      curuser := current_user;
      EXECUTE 'SET LOCAL ROLE ' || current_database() || '__rolemaster';
      UPDATE agent SET last_login = now() AT TIME ZONE 'UTC', confirmed = true WHERE id=agent_id;
      EXECUTE 'SET LOCAL ROLE ' || curuser;
      RETURN t;
    END;
    $$;

CREATE OR REPLACE FUNCTION public.send_login_email(email varchar) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
    DECLARE curuser varchar;
    DECLARE id BIGINT;
    DECLARE confirmed boolean;
    DECLARE last_login_email_sent timestamp with time zone;
    DECLARE passh varchar;
    BEGIN
      curuser := current_user;
      EXECUTE 'SET LOCAL ROLE ' || current_database() || '__rolemaster';
      SELECT m.id, m.confirmed, m.last_login_email_sent
        INTO id, confirmed, last_login_email_sent
        FROM agent as m WHERE m.email = send_login_email.email;
      IF id IS NOT NULL THEN
        IF last_login_email_sent IS NOT NULL AND now() AT TIME ZONE 'UTC' - last_login_email_sent < '@1M' THEN
          RAISE EXCEPTION 'too soon';  -- TODO: ensure base format
        END IF;
        SELECT sign(row_to_json(r), current_setting('app.jwt_secret')) INTO STRICT passh FROM (
            SELECT CONCAT('agent:', id::varchar) AS sub, extract(epoch from now())::integer + 10000 AS exp) r;
        PERFORM pg_notify(current_database(), concat('E email ', id, ' ', email, ' ',confirmed, ' ',passh));
      END IF;
      EXECUTE 'SET LOCAL ROLE ' || curuser;
      RETURN true;
    END;
    $$;


GRANT EXECUTE ON FUNCTION send_login_email(character varying) TO :dbc;

--
-- Name: after_create_agent(); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.after_create_agent() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE curuser varchar;
    DECLARE newagent varchar;
    DECLARE temp boolean;
    BEGIN
      newagent := current_database() || '__m_' || NEW.id;
      curuser := current_user;
      EXECUTE 'SET LOCAL ROLE ' || current_database() || '__rolemaster';
      EXECUTE 'CREATE ROLE ' || newagent || ' INHERIT IN GROUP ' || current_database() || '__member';
      EXECUTE 'ALTER GROUP ' || newagent || ' ADD USER ' || current_database() || '__client';
      IF 'admin' = ANY (NEW.permissions) THEN
        EXECUTE 'ALTER GROUP '||current_database()||'__admin ADD USER ' || newagent;
      END IF;
      EXECUTE 'SET LOCAL ROLE ' || curuser;
      SELECT send_login_email(NEW.email) INTO temp;
      RETURN NEW;
    END;
    $$;

DROP TRIGGER IF EXISTS after_create_agent ON public.agent;
CREATE TRIGGER after_create_agent AFTER INSERT ON public.agent FOR EACH ROW EXECUTE FUNCTION public.after_create_agent();

--
-- Name: before_update_agent(); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.before_update_agent() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE curuser varchar;
    BEGIN
      curuser := current_user;
      IF NEW.passwd != OLD.passwd THEN
        IF NEW.id != current_agent_id() THEN
          RAISE EXCEPTION 'permission / change user password';
        END IF;
        NEW.passwd = crypt(NEW.passwd, gen_salt('bf'));
      END IF;
      IF ('admin' = ANY(NEW.permissions)) != ('admin' = ANY(OLD.permissions)) AND NOT public.is_superadmin() THEN
        RAISE EXCEPTION 'permission admin / change user permissions';
      END IF;
      EXECUTE 'SET LOCAL ROLE ' || current_database() || '__rolemaster';
      IF ('admin' = ANY(NEW.permissions)) AND NOT ('admin' = ANY(OLD.permissions)) THEN
        EXECUTE 'ALTER GROUP '||current_database()||'__admin ADD USER ' || current_database() || '__m_' || NEW.id;
      END IF;
      IF ('admin' = ANY(OLD.permissions)) AND NOT ('admin' = ANY(NEW.permissions)) THEN
        EXECUTE 'ALTER GROUP '||current_database()||'__admin DROP USER ' || current_database() || '__m_' || NEW.id;
      END IF;
      EXECUTE 'SET LOCAL ROLE ' || curuser;
      RETURN NEW;
    END;
    $$;

DROP TRIGGER IF EXISTS before_update_agent ON public.agent;
CREATE TRIGGER before_update_agent BEFORE UPDATE ON public.agent FOR EACH ROW EXECUTE FUNCTION public.before_update_agent();


CREATE OR REPLACE FUNCTION public.before_create_agent() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE num_mem integer;
    BEGIN
      SELECT count(id) INTO STRICT num_mem FROM public_agent;
      IF num_mem <= 1 THEN
        -- give admin to first registered user (after system)
        NEW.permissions = ARRAY['admin'::permission];
        NEW.confirmed = true;
      ELSE
        IF NOT public.is_superadmin() THEN
          NEW.permissions = ARRAY[]::permission[];
          NEW.confirmed = false;
        END IF;
      END IF;
      RETURN NEW;
    END;
    $$;

DROP TRIGGER IF EXISTS before_create_agent ON public.agent;
CREATE TRIGGER before_create_agent BEFORE INSERT ON public.agent FOR EACH ROW EXECUTE FUNCTION public.before_create_agent();


CREATE OR REPLACE FUNCTION create_agent(
  email character varying, password character varying, username character varying,
  permissions permission[] DEFAULT ARRAY[]::permission[]
  ) RETURNS INTEGER VOLATILE AS $$
  INSERT INTO agent (email, passwd, username, permissions) VALUES ($1, crypt($2, gen_salt('bf')), $3, $4);
  -- cannot use RETURNING because of select permissions
  SELECT id FROM public_agent WHERE username=$3;
$$ LANGUAGE SQL;

GRANT EXECUTE ON FUNCTION create_agent(character varying, character varying, character varying, public.permission[]) TO :dbc;

--
-- Name: after_delete_agent(); Type: FUNCTION
--

CREATE OR REPLACE FUNCTION public.after_delete_agent() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE database varchar;
    DECLARE oldagent varchar;
    DECLARE curuser varchar;
    BEGIN
      database := current_database();
      curuser := current_user;
      oldagent := database || '__m_' || OLD.id;
      EXECUTE 'SET LOCAL ROLE ' || current_database() || '__rolemaster';
      EXECUTE 'DROP ROLE ' || oldagent;
      EXECUTE 'SET LOCAL ROLE ' || curuser;
      RETURN NEW;
    END;
    $$;


DROP TRIGGER IF EXISTS after_delete_agent ON public.agent;
CREATE TRIGGER after_delete_agent AFTER DELETE ON public.agent FOR EACH ROW EXECUTE FUNCTION public.after_delete_agent();



ALTER TABLE public.agent ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS agent_update_policy ON public.agent;
CREATE POLICY agent_update_policy ON public.agent FOR UPDATE USING (id = current_agent_id() OR public.is_superadmin());
DROP POLICY IF EXISTS agent_delete_policy ON public.agent;
CREATE POLICY agent_delete_policy ON public.agent FOR DELETE USING (id = current_agent_id() OR public.is_superadmin());
DROP POLICY IF EXISTS agent_insert_policy ON public.agent;
CREATE POLICY agent_insert_policy ON public.agent FOR INSERT WITH CHECK (true);
DROP POLICY IF EXISTS agent_select_policy ON public.agent;
CREATE POLICY agent_select_policy ON public.agent FOR SELECT USING (id = current_agent_id() OR public.is_superadmin());


COMMIT;
