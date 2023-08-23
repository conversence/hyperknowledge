"""The FastAPI server"""
from typing import List, Dict, Annotated, Tuple, Union, Optional
from contextlib import asynccontextmanager
from datetime import timedelta, datetime
from json import JSONDecodeError
from itertools import chain
import logging

from typing_extensions import TypedDict
from pydantic import ConfigDict, BaseModel
from sqlalchemy import select, text, delete
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm import joinedload
from fastapi import FastAPI, HTTPException, Request, Depends, status, Response, Query
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.websockets import WebSocket
from fastapi.responses import HTMLResponse
from starlette.websockets import WebSocketDisconnect


from .. import ClientSession, Session, production
from . import PydanticURIRef
from .context import Context
from .auth import agent_session, get_current_agent
from .schemas import (
    LocalSourceModel, RemoteSourceModel, GenericEventModel, getEventModel, AgentModel, AgentModelWithPw, EventSchema, EventHandlerSchema, EventHandlerSchemas,
    models_from_schemas, HkSchema, getProjectionSchemas, EntityTopicSchema, AgentModelOptional,
    AgentSourceSelectivePermissionModel, AgentSourcePermissionModel, AgentSourceSelectivePermissionModelOptional, AgentSourcePermissionModelOptional)
from .models import (Source, Event, Struct, UUIDentifier, Term, Vocabulary, Topic, EventHandler, Agent, AgentSourceSelectivePermission, AgentSourcePermission, schema_defines_table)
from .make_tables import read_existing_projections, KNOWN_DB_MODELS, process_schema, db_to_projection
from .auth import (
    get_token, Token, CurrentAgentType, CurrentActiveAgentType)
from .processor import start_listen_thread, stop_listen_thread, forget_handler, WebSocketDispatcher

log = logging.getLogger()

async def populate_app(app: FastAPI, initial=False):
    global KNOWN_DB_MODELS
    if initial:
        schemas, bases = await read_existing_projections()
        ps = models_from_schemas(schemas)
    EventSchema: type[GenericEventModel] = getEventModel()

    # Clear the old routes
    for i, r in reversed(list(enumerate(app.router.routes))):
        if r.path == '/source/{source_name}/events' or \
                r.path.startswith("/source/{source_name}/topic/{entity_id}/"):
            del app.router.routes[i]

    @app.get("/source/{source_name}/events")
    async def get_events(
            source_name: str,
            current_agent: CurrentAgentType,
            limit: int=50, after: datetime=None) -> List[EventSchema]:
        ev_class = getEventModel()
        async with agent_session(current_agent) as session:
            q = select(Event).join(Source).filter_by(local_name=source_name).order_by(Event.created).options(joinedload(Event.creator), joinedload(Event.source))
            if after:
                q = q.filter(Event.created > after)
            events = await session.execute(q.limit(limit))
            # Add the context to each event?
            return [ev_class.model_validate(ev) for (ev,) in events]

    @app.post("/source/{source_name}/events")
    async def post_event(
            source_name: str,
            event: EventSchema,
            request: Request,
            response: Response,
            current_agent: CurrentActiveAgentType
          ) -> EventSchema:
        event_json = await request.json()
        data = event_json['data']
        async with agent_session(current_agent) as session:
            r = await session.execute(select(Source).filter_by(local_name=source_name))
            if not (r:= r.first()):
                raise HTTPException(404, f"Source {source_name} does not exist")
            source = r[0]
            if event.source:
                assert event.source == PydanticURIRef(source.uri)
            else:
                event.source = PydanticURIRef(source.uri)
            if event.creator:
                assert event.creator == current_agent.username
            else:
                event.creator = current_agent.username
            # Get the event type
            prefix, type_term = event.data.type.split(':')
            event_type = await Term.ensure(session, type_term, None, prefix)
            # Get the event's schema
            await session.refresh(event_type, ['schema'])
            hk_schema = HkSchema.model_validate(event_type.schema.value)
            event_schema = hk_schema.eventSchemas[type_term]
            # If we are supposed to create a resource, now is a good time to do so
            try:
                for attrib in event_schema.attributes:
                    if attrib.create and not getattr(event.data, attrib.name):
                        resourceName = await UUIDentifier.ensure(session)
                        # TODO: Check if the type is a projection type
                        range_prefix, range_term = hk_schema.context.shrink_iri(attrib.range).split(':')
                        resourceType = await Term.ensure(session, range_term, None, range_prefix)
                        resourceName.has_projections = [resourceType.id]
                        data[attrib.name] = resourceName.uri
                        setattr(event.data, attrib.name, resourceName.uri)
                        # Should we fill the projection table now? I think that is best delayed to ingestion
                # TODO: Compute the indices of all topics. Wait, no column in event? Only in topic.
            except Exception as e:
                log.exception(e)
                raise e
            db_event = Event(source=source, data=data, creator_id=current_agent.id, event_type=event_type)
            session.add(db_event)
            await session.commit()
        event.created = db_event.created
        response.status_code = status.HTTP_201_CREATED
        response.headers["Location"] = f"/source/{source_name}/events/{event.created}"
        return event

    def define_projection_getter(topic, schema, db_model, pyd_model):
        @app.get("/source/{source_name}/topic/{entity_id}/"+f"{topic.vocabulary.prefix}:{schema.name}")
        async def get_projection(source_name: str, entity_id: str, request: Request, response: Response, current_agent: CurrentAgentType) -> pyd_model:
            async with agent_session(current_agent) as session:
                q = select(db_model).filter_by(obsolete=None).join(UUIDentifier).filter_by(value=entity_id).join(Source, db_model.source_id==Source.id).filter_by(local_name=source_name)
                projection = await session.execute(q)
                if projection := projection.first():
                    model = await db_to_projection(session, projection[0], pyd_model, schema)
                    response.headers["Link"] = f'<{request.base_url}schema/{schema_id}/context>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
                    return model
            raise HTTPException(404, "Could not find the projection")

    async with Session() as session:
        for schema_type, (schema, pyd_model) in getProjectionSchemas().items():
            topic = await Topic.get_by_uri(session, schema_type)
            schema_id = await session.scalar(select(schema_defines_table.c.schema_id).filter(schema_defines_table.c.term_id==topic.id))
            db_model = KNOWN_DB_MODELS.get(schema_type)
            if not db_model:
                log.warning("Missing model!", schema_type)
                continue
            # Wrap in a closure to avoid variable interference
            define_projection_getter(topic, schema, db_model, pyd_model)

    app.openapi_schema = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # populate with dynamic classes
    await populate_app(app, True)
    start_listen_thread()
    yield
    # Clean up
    stop_listen_thread()


app = FastAPI(lifespan=lifespan)

ACCESS_TOKEN_EXPIRE_MINUTES = 30

@app.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
):
    token = await get_token(form_data.username, form_data.password)
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return {"access_token": token, "token_type": "bearer"}


@app.get("/agents/me/", response_model=AgentModel)
async def read_agents_me(
    current_agent: CurrentActiveAgentType
):
    return current_agent

@app.post("/agents")
async def add_agent(model: AgentModelWithPw, response: Response, current_agent: CurrentAgentType) -> None:
    async with agent_session(current_agent) as session:
        # TODO: Password integrity rules
        value = await session.scalar(func.create_agent(model.email, model.passwd, model.username, model.permissions))
        await session.commit()
        response.status_code = status.HTTP_201_CREATED
        # location is not public... maybe for admin?


@app.get("/agents")
async def get_agents(current_agent: CurrentActiveAgentType) -> List[AgentModel]:
    print('get_agents')
    if  not current_agent.has_permission('admin'):
        raise HTTPException(status_code=403, detail="Admin access required")
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Agent))
        print(r)
        return [AgentModel.model_validate(x) for (x,) in r]


@app.patch("/agents/{username}")
async def patch_agent(
        model: AgentModelOptional, username: str, response: Response,
        current_agent: CurrentActiveAgentType) -> AgentModel:
    if not (current_agent.username == username or 'admin' in current_agent.permissions):
        raise HTTPException(401, "Cannot modify another agent")
    async with agent_session(current_agent) as session:
        agent = await session.execute(select(Agent).filter_by(username=username))
        agent = agent.first()
        if not agent:
            raise HTTPException(404, "No such agent")
        agent = agent[0]

        for key, value in model.model_dump(exclude_unset=True).items():
            setattr(agent, key, value)
        await session.commit()
        return AgentModel.model_validate(agent)


@app.get("/remote_source")
async def get_remote_sources(current_agent: CurrentAgentType) -> Dict[int, PydanticURIRef]:
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Source).filter_by(local_name=None))
        return {s.id: s.uri for (s,) in r}


@app.get("/remote_source/{source_id}")
async def get_remote_source(source_id: int, current_agent: CurrentAgentType) -> RemoteSourceModel:
    async with agent_session(current_agent) as session:
        if source := await session.get(Source, source_id):
            return RemoteSourceModel.model_validate(source)
        raise HTTPException(status_code=404, detail="Source does not exist")

@app.post("/remote_source")
async def add_remote_source(model: RemoteSourceModel, response: Response, current_agent: CurrentActiveAgentType
        ) -> RemoteSourceModel:
    if  not current_agent.has_permission('add_source'):
        raise HTTPException(401)
    async with agent_session(current_agent) as session:
        if source.local_name:
            raise HTTPException(400, "Remote sources cannot have a local name")
        if not source.uri:
            raise HTTPException(400, "Remote sources need a URI")
        source = await Source.ensure(session, model.uri, None, model.prefix)
        await session.commit()
        response.status_code = status.HTTP_201_CREATED
        response.headers["Location"] = f"/remote_source/{source.id}"
        return RemoteSourceModel.model_validate(source)

@app.get("/source")
async def get_local_sources(current_agent: CurrentAgentType) -> Dict[str, PydanticURIRef]:
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Source).filter(Source.local_name != None))
        return {s.local_name: s.uri for (s,) in r}


@app.get("/source/{source_name}")
async def get_local_source(source_name: str, current_agent: CurrentAgentType) -> LocalSourceModel:
    async with agent_session(current_agent) as session:
        source = await session.execute(select(Source).filter_by(local_name=source_name))
        if source := source.first():
            return LocalSourceModel.model_validate(source[0])
        raise HTTPException(status_code=404, detail="Source does not exist")

@app.post("/source")
async def add_local_source(
        model: LocalSourceModel, request: Request, response: Response,
        current_agent: CurrentActiveAgentType) -> LocalSourceModel:
    if  not current_agent.has_permission('add_source'):
        raise HTTPException(401)
    async with agent_session(current_agent) as session:
        uri = f"{request.base_url}/source/{model.local_name}"
        if model.included_source_ids:
            can_read = await session.execute(select(and_(*[func.can_read_source(source_id) for source_id in model.included_source_ids])))
            if not can_read:
                raise HTTPException(403, "Cannot read included sources")
        source = await Source.ensure(session, uri, model.local_name, model.public_read, model.public_write, model.selective_write, model.included_source_ids)
        await session.commit()
        response.status_code = status.HTTP_201_CREATED
        response.headers["Location"] = f"/source/{source.local_name}"
        await session.refresh(source, ["creator", "included_source_ids"])
        return LocalSourceModel.model_validate(source)


@app.get("/source/{source_name}/permission")
async def get_local_source_permissions(source_name: str, current_agent: CurrentActiveAgentType) -> List[Union[AgentSourceSelectivePermissionModel, AgentSourcePermissionModel]]:
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Source).filter_by(local_name=source_name))
        r = r.first()
        if not r:
            raise HTTPException(status_code=404, detail="Source does not exist")
        source: Source = r[0]
        q = select(AgentSourcePermission).filter_by(source_id=source.id).options(
            joinedload(AgentSourcePermission.agent),
            joinedload(AgentSourcePermission.source))
        if source.creator_id != current_agent.id:
            q = q.filter_by(agent_id=current_agent.id)
        asps = await session.execute(q)
        asps = [AgentSourcePermissionModel.model_validate(asp) for (asp,) in asps]
        q = select(AgentSourceSelectivePermission).filter_by(source_id=source.id).options(
            joinedload(AgentSourceSelectivePermission.agent),
            joinedload(AgentSourceSelectivePermission.source),
            joinedload(AgentSourceSelectivePermission.event_type))
        if source.creator_id != current_agent.id:
            q = q.filter_by(agent_id=current_agent.id)
        assps = await session.execute(q)
        assps = [AgentSourceSelectivePermissionModel.model_validate(assp) for (assp,) in assps]
        return chain(asps, assps)


@app.post("/source/{source_name}/permission")
async def add_local_source_permissions(
        source_name: str, permission: Union[AgentSourceSelectivePermissionModelOptional, AgentSourcePermissionModelOptional], current_agent: CurrentActiveAgentType, request: Request, response: Response
        ) -> Union[AgentSourceSelectivePermissionModel, AgentSourcePermissionModel]:
    permission_uri = PydanticURIRef(f"{request.base_url}/source/{source_name}")
    if permission.source and permission.source != permission_uri:
        raise HTTPException(status_code=404, detail="Posting on the wrong source")
    permission.source = permission_uri
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Source).filter_by(local_name=source_name))
        r = r.first()
        if not r:
            raise HTTPException(status_code=404, detail="Source does not exist")
        source: Source = r[0]
        if permission.agent and permission.agent != current_agent.username:
            r = await session.execute(select(Agent).filter_by(username=permission.agent))
            r = r.first()
            if not r:
                raise HTTPException(status_code=404, detail="Agent does not exist")
            agent: Agent = r[0]
        else:
            agent = current_agent
            permission.agent = current_agent.username
        args = permission.model_dump()
        args.update(dict(source=source, agent=agent))
        if getattr(permission, "event_type", None):
            event_type = await Term.get_by_uri(session, permission.event_type)
            if not event_type:
                raise HTTPException(status_code=404, detail="event_type does not exist")
            args[event_type] = event_type
            model = AgentSourceSelectivePermission
        else:
            model = AgentSourcePermission
        permissionOb = model(**args)
        # TODO: check permissions here for better error messages
        session.add(permissionOb)
        await session.commit()
    response.status_code = status.HTTP_201_CREATED
    return permission.__class__.model_validate(permissionOb)


@app.patch("/source/{source_name}/permission")
async def update_local_source_permissions(
        source_name: str, permission: Union[AgentSourceSelectivePermissionModelOptional, AgentSourcePermissionModelOptional], current_agent: CurrentActiveAgentType, request: Request
        ) -> Union[AgentSourceSelectivePermissionModel, AgentSourcePermissionModel]:
    permission_uri = PydanticURIRef(f"{request.base_url}/source/{source_name}")
    if permission.source and permission.source != permission_uri:
        raise HTTPException(status_code=404, detail="Posting on the wrong source")
    permission.source = permission_uri
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Source).filter_by(local_name=source_name))
        r = r.first()
        if not r:
            raise HTTPException(status_code=404, detail="Source does not exist")
        source: Source = r[0]
        if permission.agent and permission.agent != current_agent.username:
            r = await session.execute(select(Agent).filter_by(username=permission.agent))
            r = r.first()
            if not r:
                raise HTTPException(status_code=404, detail="Agent does not exist")
            agent: Agent = r[0]
        else:
            agent = current_agent
            permission.agent = current_agent.username
        if getattr(permission, "event_type", None):
            event_type = await Term.get_by_uri(session, permission.event_type)
            if not event_type:
                raise HTTPException(status_code=404, detail="event_type does not exist")

            r = await session.execute(select(AgentSourceSelectivePermission).filter_by(source=source, agent=agent, event_type=event_type))
            r = r.first()
            if not r:
                raise HTTPException(status_code=404, detail="Permission does not exist")
            permissionOb = r[0]
            permissionOb.source = source
            permissionOb.agent = agent
            permissionOb.event_type = event_type
        else:
            r = await session.execute(select(AgentSourcePermission).filter_by(source=source, agent=agent))
            r = r.first()
            if not r:
                raise HTTPException(status_code=404, detail="Permission does not exist")
            permissionOb = r[0]
            permissionOb.source = source
            permissionOb.agent = agent

        args = permission.model_dump()
        args.pop('source')
        args.pop('agent')
        args.pop('event_type', None)
        for k, v in args.items():
            if v is not None:
                setattr(permissionOb, k, v)
        # TODO: check permissions here for better error messages
        await session.commit()
        return permission.__class__.model_validate(permissionOb)

@app.delete("/source/{source_name}/permission")
async def delete_local_source_permissions(
        source_name: str, permission: Union[AgentSourceSelectivePermissionModelOptional, AgentSourcePermissionModelOptional], current_agent: CurrentActiveAgentType, request: Request):
    permission_uri = PydanticURIRef(f"{request.base_url}/source/{source_name}")
    if permission.source and permission.source != permission_uri:
        raise HTTPException(status_code=404, detail="Posting on the wrong source")
    permission.source = permission_uri
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Source).filter_by(local_name=source_name))
        r = r.first()
        if not r:
            raise HTTPException(status_code=404, detail="Source does not exist")
        source: Source = r[0]
        if permission.agent and permission.agent != current_agent.username:
            r = await session.execute(select(Agent).filter_by(username=permission.agent))
            r = r.first()
            if not r:
                raise HTTPException(status_code=404, detail="Agent does not exist")
            agent: Agent = r[0]
        else:
            agent = current_agent
            permission.agent = current_agent.username
        if getattr(permission, "event_type", None):
            event_type = await Term.get_by_uri(session, permission.event_type)
            if not event_type:
                raise HTTPException(status_code=404, detail="event_type does not exist")

            await session.execute(delete(AgentSourceSelectivePermission).filter(AgentSourceSelectivePermission.source_id==source.id, AgentSourceSelectivePermission.agent_id==agent.id, AgentSourceSelectivePermission.event_type_id==event_type.id))
        else:
            await session.execute(delete(AgentSourcePermission).filter(AgentSourcePermission.source_id==source.id, AgentSourcePermission.agent_id==agent.id))
        await session.commit()


@app.post("/schema")
async def add_schema(schema: HkSchema, request: Request, response: Response, current_agent: CurrentActiveAgentType):
    if  not current_agent.has_permission('add_schema'):
        raise HTTPException(401)
    schema_json = await request.json()
    ctx = schema.context
    base = getattr(ctx, 'vocab', None) or getattr(ctx, '_base', None)
    if not base:
        raise HTTPException(status_code=400, detail="The schema needs to have a _base or vocab")
    prefix = ctx._prefixes[base]
    if not prefix:
        raise HTTPException(status_code=400, detail="The schema base needs to have a prefix")
    async with agent_session(current_agent) as session:
        db_schema = await process_schema(schema, schema_json, base, prefix, session=session)
        await session.commit()
    await populate_app(app)

    response.status_code = status.HTTP_201_CREATED
    response.headers["Location"] = f"/schema/{db_schema.id}"
    return getEventModel().model_json_schema()


@app.get("/schema")
async def get_schema_ids_and_prefixes(current_agent: CurrentAgentType) -> List[Tuple[int, str]]:
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Struct.id, Vocabulary.prefix).filter_by(subtype='hk_schema').join(Struct.as_voc))
        return r.all()


@app.get("/schema/{schema_id}")
async def get_schema(schema_id: int, current_agent: CurrentAgentType) -> HkSchema:
    async with agent_session(current_agent) as session:
        s = await session.get(Struct, schema_id)
        if s:
            if s.subtype != 'hk_schema':
                raise HTTPException(status_code=400, detail="Not a schema")
            m = HkSchema.model_validate(s.value)
            # keep the original context form
            m.context = s.value['@context']
            return m
        raise HTTPException(status_code=404, detail="Schema does not exist")


@app.delete("/schema/{schema_id}")
async def delete_schema(schema_id: int, current_agent: CurrentActiveAgentType):
    if  not current_agent.has_permission('admin'):
        raise HTTPException(401)
    async with agent_session(current_agent) as session:
        s = await session.get(Struct, schema_id)
        if s:
            if s.subtype != 'hk_schema':
                raise HTTPException(status_code=400, detail="Not a schema")
            session.delete(s)
            await session.commit()
            return Response(204)
        raise HTTPException(status_code=404, detail="Schema does not exist")


@app.get("/schema/p/{prefix}")
async def get_schema_by_prefix(prefix: str, current_agent: CurrentAgentType) -> List[HkSchema]:
    async with agent_session(current_agent) as session:
        r = await session.execute(select(Struct).filter_by(subtype='hk_schema').join(Struct.as_voc).filter_by(prefix=prefix))
        # TODO: What if there's more than one? There may be a design mistake here.
        schemas = [s for (s,) in r]
        models = [HkSchema.model_validate(s.value) for s in schemas]
        for i, s in enumerate(schemas):
            models[i].context = s.value['@context']
        return models



@app.get("/schema/{schema_id}/context")
async def get_schema_context(schema_id: int, current_agent: CurrentAgentType) -> Dict:
    async with agent_session(current_agent) as session:
        s = await session.get(Struct, schema_id)
        if s:
            if s.subtype != 'hk_schema':
                raise HTTPException(status_code=400, detail="Not a schema")
            return {"@context": s.value["@context"]}
        raise HTTPException(status_code=404, detail="Schema does not exist")


@app.get("/schema/{prefix}/{component}")
async def get_schema_context(prefix: str, component:str, current_agent: CurrentAgentType) -> EventSchema:
    async with agent_session(current_agent) as session:
        r = await session.execute(
            select(Struct).filter_by(subtype='hk_schema'
                ).join(Struct.terms).filter_by(term=component
                ).join(Term.vocabulary).filter_by(prefix=prefix))
        if r:= r.one_or_none():
            s: Struct = r[0]
            m = HkSchema.model_validate(s.value)
            return m.eventSchemas.get(component) or m.projectionSchemas.get(component)
        raise HTTPException(status_code=404, detail="Schema does not exist")


@app.get("/schema/{prefix}/{component}/js")
async def get_schema_context(prefix: str, component:str, current_agent: CurrentAgentType) -> Dict:
    async with agent_session(current_agent) as session:
        r = await session.execute(
            select(Struct).filter_by(subtype='hk_schema'
                ).join(Struct.terms).filter_by(term=component
                ).join(Term.vocabulary).filter_by(prefix=prefix))
        if r:= r.one_or_none():
            s: Struct = r[0]
            m = HkSchema.model_validate(s.value)
            c_schema = m.eventSchemas.get(component) or m.projectionSchemas.get(component)
            return c_schema.model_json_schema()
        raise HTTPException(status_code=404, detail="Schema does not exist")


@app.get("/source/{source_name}/topic/{entity_id}")
async def get_entity(source_name: str, entity_id: str, current_agent: CurrentAgentType) -> EntityTopicSchema:
    async with agent_session(current_agent) as session:
        # WTF?
        projection = await session.execute(select(UUIDentifier).filter_by(source_id=source_id, obsolete=None).join(UUIDentifier).filter_by(value=entity_id))
        if projection := projection.first():
            return EntityTopicSchema.model_validate(projection[0])
        raise HTTPException(404, "Entity does not exist")

@app.get("/context")
async def get_contexts(current_agent: CurrentAgentType) -> Dict[int, PydanticURIRef]:
    async with agent_session(current_agent) as session:
        contexts = await session.execute(select(Struct).filter_by(subtype='ld_context').options(joinedload(Struct.as_voc)))
        return {r.id: r.as_voc.uri for (r,) in contexts}


@app.get("/context/{ctx_id}")
async def get_context(ctx_id: int, response: Response, current_agent: CurrentAgentType) -> Dict:
    async with agent_session(current_agent) as session:
        context = await session.get(Struct, ctx_id)
        if not context:
            raise HTTPException(401, "Context does not exist")
        if context.subtype != 'ld_context':
            raise HTTPException(400, "Not a context")
        await session.refresh(context, ['as_voc'])
        response.headers["Link"] = f'<{context.as_voc.uri}>; rel="source"; type="application/ld+json"'
        return context.value

class ContextData(TypedDict):
    # __pydantic_config__ = ConfigDict(extra='forbid')
    ctx: Optional[Dict]
    url: str

@app.post("/context")
async def add_context(data: ContextData, response: Response, current_agent: CurrentActiveAgentType):
    url = data['url']
    ctx = data.get('ctx')
    async with agent_session(current_agent) as session:
        # First make sure it's valid
        ld_context = Context(ctx, base=url)
        vocab = await Vocabulary.ensure(session, url)
        r = await session.execute(select(Struct).filter_by(is_vocab=vocab.id, subtype='ld_context'))
        if context_struct := r.first():
            context_struct = context_struct[0]
            # Probably don't overwrite... unless user is admin?
            # context_struct.value = ctx
        else:
            context_struct = Struct(value=ctx, subtype='ld_context', is_vocab=vocab.id)
            session.add(context_struct)
        await session.commit()
        response.status_code = status.HTTP_201_CREATED
        response.headers["Location"] = f"/context/{context_struct.id}"
        return context_struct

@app.get("/handler")
async def get_handlers(current_agent: CurrentAgentType) -> Dict[int, Tuple[str, str, str]]:
    async with agent_session(current_agent) as session:
        handlers = await session.execute(select(EventHandler).options(
            joinedload(EventHandler.event_type).joinedload(Term.vocabulary),
            joinedload(EventHandler.target_range).joinedload(Term.vocabulary)))
        return {eh.id: (eh.event_type.uri, eh.target_role, eh.target_range.uri)
                for (eh,) in handlers}

@app.get("/handler/{handler_id}")
async def get_handler(handler_id: int, current_agent: CurrentAgentType) -> EventHandlerSchema:
    async with agent_session(current_agent) as session:
        handler = await session.get(EventHandler, handler_id, options=(
            joinedload(EventHandler.event_type).joinedload(Term.vocabulary),
            joinedload(EventHandler.target_range).joinedload(Term.vocabulary)))
        if not handler:
            return HTTPException(404, "No such handler")
        data = dict(handler.__dict__,
            event_type=handler.event_type.uri, target_range=handler.target_range.uri
        )
        return EventHandlerSchema.model_validate(data)


@app.post("/handler")
async def add_handlers(handlers: EventHandlerSchemas, response: Response, current_agent: CurrentActiveAgentType) -> List[EventHandlerSchema]:
    if  not current_agent.has_permission('add_handler'):
        raise HTTPException(401)
    db_handlers = []
    async with agent_session(current_agent) as session:
        for handler in handlers.handlers:
            event_type = await Term.ensure_url(session, handlers.context.expand(handler.event_type), True)
            target_range = await Term.ensure_url(session, handlers.context.expand(handler.target_range), True)
            data = handler.model_dump()
            data.update(dict(event_type=event_type, target_range=target_range))
            existing = await session.execute(select(EventHandler).filter_by(
                event_type=event_type, target_range=target_range, target_role=handler.target_role))
            if existing := existing.first():
                existing = existing[0]
                existing.code_text = handler.code_text
                existing.language = handler.language
                forget_handler(existing.id)
            else:
                db_handlers.append(EventHandler(**data))
        session.add_all(db_handlers)
        await session.commit()
    response.status_code = status.HTTP_201_CREATED
    return [EventHandlerSchema.model_validate(h) for h in db_handlers]


if not production:
    with open("static/ws_test.html") as f:
        web_test_html = f.read()

    @app.get("/wstest")
    async def get():
        return HTMLResponse(web_test_html)


@app.websocket("/ws")
async def websocket(websocket: WebSocket):
    await websocket.accept()
    # Deciding to do the login within the WebSocket rather than use cookies.
    while True:
        try:
            token_data = await websocket.receive_json()
        except JSONDecodeError as e:
            await websocket.send_json(dict(error=f"Invalid JSON {e.msg}"))
            continue
        except WebSocketDisconnect as e:
            return
        token = token_data.get('token')
        if not token:
            await websocket.send_json(dict(error="first provide a login token"))
            continue
        agent_model = None
        try:
            agent_model = await get_current_agent(token)
        except Exception as e:
            log.exception(e)
        if agent_model:
            await websocket.send_json(dict(login=agent_model.username))
            break
        await websocket.send_json(dict(error="Invalid login token"))
    handler = WebSocketDispatcher.dispatcher.add_socket(websocket, agent_model)
    await handler.handle_ws()
