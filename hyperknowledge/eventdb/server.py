"""The FastAPI server"""
from typing import List, Dict, Annotated, Tuple, Union, Optional
from contextlib import asynccontextmanager
from datetime import timedelta, datetime
import logging

from typing_extensions import TypedDict
from pydantic import ConfigDict
from sqlalchemy import select, text
from sqlalchemy.sql.functions import func
from sqlalchemy.orm import joinedload
from fastapi import FastAPI, HTTPException, Request, Depends, status, Response
from fastapi.security import OAuth2PasswordRequestForm

from .. import ClientSession, Session
from . import PydanticURIRef
from .context import Context
from .auth import agent_session
from .schemas import (
    LocalSourceModel, RemoteSourceModel, GenericEventModel, getEventModel, AgentModel, AgentModelWithPw, EventSchema, EventHandlerSchema, EventHandlerSchemas,
    models_from_schemas, HkSchema, getProjectionSchemas, EntityTopicSchema, AgentModelOptional)
from .models import (Source, Event, Struct, UUIDentifier, Term, Vocabulary, Topic, EventHandler, Agent, schema_defines_table)
from .make_tables import read_existing_projections, KNOWN_DB_MODELS, process_schema, db_to_projection
from .auth import (
    authenticate_agent, create_access_token, Token, CurrentAgentType, CurrentActiveAgentType)
from .projection_processor import start_listen_thread, stop_listen_thread, forget_handler

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
    async with ClientSession() as session:
        agent = await authenticate_agent(session, form_data.username, form_data.password)
    if not agent:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": f'agent:{agent.id}'}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/agents/me/", response_model=AgentModel)
async def read_agents_me(
    current_agent: CurrentActiveAgentType
):
    return current_agent

@app.post("/agents")
async def add_agent(model: AgentModelWithPw, response: Response, current_agent: CurrentAgentType) -> None:
    async with agent_session(current_agent) as session:
        # TODO: Password integrity rules
        value = await session.scalar(func.create_agent(model.email, model.passwd, model.username, model.is_admin))
        await session.commit()
        response.status_code = status.HTTP_201_CREATED
        # location is not public


@app.patch("/agents/{username}")
async def patch_agent(
        model: AgentModelOptional, username: str, response: Response,
        current_agent: CurrentActiveAgentType) -> AgentModel:
    if not (current_agent.username == username or current_agent.is_admin):
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
async def add_remote_source(model: RemoteSourceModel, response: Response, current_agent: CurrentActiveAgentType):
    async with agent_session(current_agent) as session:
        if source.local_name:
            raise HTTPException(400, "Remote sources cannot have a local name")
        if not source.uri:
            raise HTTPException(400, "Remote sources need a URI")
        source = await Source.ensure(session, model.uri, None, model.prefix)
        await session.commit()
        response.status_code = status.HTTP_201_CREATED
        response.headers["Location"] = f"/remote_source/{source.id}"

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
        current_agent: CurrentActiveAgentType):
    async with agent_session(current_agent) as session:
        uri = f"{request.base_url}/source/{model.local_name}"
        source = await Source.ensure(session, uri, model.local_name, None)
        await session.commit()
        response.status_code = status.HTTP_201_CREATED
        response.headers["Location"] = f"/source/{source.local_name}"


@app.post("/schema")
async def add_schema(schema: HkSchema, request: Request, response: Response, current_agent: CurrentActiveAgentType):
    if not current_agent.is_admin:
        # TODO: a permission lower than admin to add schemas
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
async def add_context(data: ContextData, response: Response, current_agent: CurrentActiveAgentType) -> int:
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
        return context_struct.id

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
async def add_handlers(handlers: EventHandlerSchemas, response: Response, current_agent: CurrentActiveAgentType) -> List[int]:
    if not current_agent.is_admin:
        # TODO: a permission lower than admin to add handlers
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
    return [h.id for h in db_handlers]
