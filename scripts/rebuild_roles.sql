BEGIN;

DO $$
DECLARE role_name VARCHAR;
DECLARE entity_id INTEGER;
DECLARE member_id_ INTEGER;
DECLARE temp BOOLEAN;
DECLARE curuser VARCHAR;
BEGIN
  curuser := current_user;
  EXECUTE 'SET LOCAL ROLE ' || current_database() || '__rolemaster';
  -- delete all roles
  FOR role_name IN
    SELECT rolname FROM pg_catalog.pg_roles
    WHERE rolname LIKE current_database() || '\_\__\_%'
  LOOP
    EXECUTE 'DROP ROLE ' || role_name;
  END LOOP;
  -- recreate agents
  FOR member_id_, temp IN
    SELECT id, 'admin' = ANY(permissions) FROM public.agent
  LOOP
    role_name := current_database() || '__m_' || member_id_;
    EXECUTE 'CREATE ROLE ' || role_name || ' INHERIT IN GROUP ' || current_database() || '__member';
    EXECUTE 'ALTER GROUP ' || role_name || ' ADD USER ' || current_database() || '__client';
    IF temp THEN
      EXECUTE 'ALTER GROUP '||current_database()||'__admin ADD USER ' || role_name;
    END IF;
  END LOOP;
  EXECUTE 'SET LOCAL ROLE ' || curuser;
END$$;

COMMIT;
