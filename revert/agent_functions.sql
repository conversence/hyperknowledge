-- Revert sensecraft:agents_functions from pg

BEGIN;

\set dbm :dbn '__member';
\set dbc :dbn '__client';

DELETE FROM public.agent;

REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE public.agent FROM :dbm;
REVOKE SELECT,INSERT ON TABLE public.agent FROM :dbc;

DROP POLICY IF EXISTS agent_update_policy ON public.agent;
DROP POLICY IF EXISTS agent_delete_policy ON public.agent;
DROP POLICY IF EXISTS agent_select_policy ON public.agent;
DROP POLICY IF EXISTS agent_insert_policy ON public.agent;
ALTER TABLE public.agent DISABLE ROW LEVEL SECURITY;
DROP TRIGGER IF EXISTS before_update_agent ON public.agent;
DROP FUNCTION IF EXISTS  public.before_update_agent();
DROP TRIGGER IF EXISTS after_delete_agent ON public.agent;
DROP FUNCTION IF EXISTS  public.after_delete_agent();
DROP TRIGGER IF EXISTS after_create_agent ON public.agent;
DROP FUNCTION IF EXISTS  public.after_create_agent();
DROP TRIGGER IF EXISTS before_create_agent ON public.agent;
DROP FUNCTION IF EXISTS  public.before_create_agent();

DROP FUNCTION IF EXISTS  public.get_token(mail character varying, pass character varying);
DROP FUNCTION IF EXISTS  public.renew_token(token character varying);
DROP FUNCTION IF EXISTS  public.send_login_email(email varchar);
DROP FUNCTION IF EXISTS  public.scagent_handle();
DROP FUNCTION IF EXISTS  public.role_to_handle(role character varying);
DROP FUNCTION IF EXISTS  public.has_permission(permission character varying);
DROP FUNCTION IF EXISTS  public.current_agent();
DROP FUNCTION IF EXISTS  public.current_agent_id();
DROP FUNCTION IF EXISTS  public.create_agent(name character varying, email character varying, password character varying, handle character varying, permissions permission[]);
DROP FUNCTION IF EXISTS  public.is_superadmin();

COMMIT;
