"""Consume the events and update the projections

This happens on a thread, with attendant machinery.
"""
import asyncio
from threading import Thread
from datetime import datetime
from typing import Dict
import logging

import anyio
import asyncpg_listen
from pydantic import Json, BaseModel
from sqlalchemy import select
from sqlalchemy.orm import subqueryload, joinedload
from sqlalchemy.dialects.postgresql import insert
from py_mini_racer import MiniRacer

from .. import target_db, config, make_scoped_session
from . import dbTopicId
from .models import (
    Base, EventProcessor, EventProcessorGlobalStatus, Event, Source, Term, Topic, EventHandler
)
from .schemas import getEventModel, HkSchema, getProjectionSchemas, ProjectionSchema
from .make_tables import KNOWN_DB_MODELS, db_to_projection, projection_to_db

log = logging.getLogger("processor")


PORTAL_ = None
Session = None
js_ctx = MiniRacer()
loaded_handlers = set()

def forget_handler(handler_id: int):
    global loaded_handlers
    loaded_handlers.discard(handler_id)


# async def get_connection():
#     engine = make_engine()
#     conn = await engine.connect()
#     conn2 = await conn.get_raw_connection()
#     return conn2.driver_connection

async def applyEvent(session, db_event: Event, creation_only:bool=False):
    global js_ctx, loaded_handlers
    ev_schema = getEventModel()
    event = ev_schema.model_validate(db_event)
    # get the event's schema
    hk_schema = HkSchema.model_validate(db_event.event_type.schema.value)
    event_type = hk_schema.context.shrink_iri(event.data.type)
    event_hk_schema = hk_schema.eventSchemas[event.data.type.split(':')[1]]
    db_model_by_role: Dict[str, type[Base]] = {}
    db_object_by_role: Dict[str, Base] = {}
    projection_by_role: Dict[str, Json] = {}
    updated_projection_by_role: Dict[str, Json] = {}
    projection_model_by_role: Dict[str, BaseModel] = {}
    projection_schemas = getProjectionSchemas()
    schemas_by_role: Dict[str, ProjectionSchema] = {}
    topic_id_by_role: Dict[str, dbTopicId] = {}
    for attrib_schema in event_hk_schema.attributes:
        value = getattr(event.data, attrib_schema.name)
        if not value:
            continue
        range_schema, range_model = projection_schemas.get(attrib_schema.range, (None, None))
        projection_model_by_role[attrib_schema.name] = range_model
        if not range_schema:
            continue
        schemas_by_role[attrib_schema.name] = range_schema
        range_db_model = KNOWN_DB_MODELS.get(range_schema.type)
        if not range_db_model:
            log.warning("Missing model!", range_schema.type)
            continue
        db_model_by_role[attrib_schema.name] = range_db_model
        entity_id = await Topic.get_by_uri(session, value)
        if not entity_id:
            log.warning("Missing topic:", value)
            continue
        topic_id_by_role[attrib_schema.name] = entity_id.id
        # TODO: Check that range's id is in topic's as_projection
        db_projection = await session.execute(select(range_db_model).filter_by(id=entity_id.id, source_id=db_event.source_id, obsolete=None))
        if db_projection := db_projection.one_or_none():
            db_projection = db_projection[0]
        else:
            log.warning("projection not found")
            continue
        pd_projection = await db_to_projection(session, db_projection, range_model, range_schema)
        db_object_by_role[attrib_schema.name] = db_projection
        projection_by_role[attrib_schema.name] = pd_projection.model_dump(by_alias=True)
    # apply the handlers
    for handler in db_event.handlers:
        if handler.language != 'javascript':
            raise NotImplementedError()
        handler_fname = f"handler_{handler.id}"
        if handler.id not in loaded_handlers:
            handler_txt = handler.code_text.replace('function handler(', f'function {handler_fname}(')
            js_ctx.eval(handler_txt)
            loaded_handlers.add(handler.id)
        js = f"{handler_fname}({event.model_dump_json(by_alias=True, exclude_unset=True)}, {projection_by_role})"
        result = js_ctx.execute(js)
        # Convenience so the handler does not have to do it
        if result["@type"] == event.data.type:
            result["@type"] = hk_schema.context.shrink_iri(handler.target_range.uri)
        # Use either create or update model according to whether it exists
        new_projection = projection_model_by_role[handler.target_role].model_validate(result)
        db_data = await projection_to_db(session, new_projection, schemas_by_role[handler.target_role])
        if handler.target_role in db_object_by_role:
            db_projection = db_object_by_role[handler.target_role]
            for k, v in db_data.items():
                setattr(db_projection, k, v)
        else:
            db_projection = db_model_by_role[handler.target_role](
                id= topic_id_by_role[handler.target_role],source_id=db_event.source_id, event_time=db_event.created ,**db_data)
            session.add(db_projection)
        # Should I update the current one? It introduces order-dependence, so no.
        # THOUGH I could do it for missing values first?
        updated_projection_by_role[handler.target_role] = new_projection.model_dump()
        # TODO: Make sure the projection type is in the topic


async def handle_notifications(notification: asyncpg_listen.NotificationOrTimeout) -> None:
    global Session
    if isinstance(notification, asyncpg_listen.Timeout):
        return
    latest_timestamp = datetime.fromisoformat((notification.payload+'00000')[:26])
    last_timestamp = None
    main_processor_id = None
    async with Session() as session:
        if last_timestamp is None:
            # "system" agent has id 0
            main_processor_id = await session.scalar(select(EventProcessor.id).filter_by(owner_id=0, name='processor'))
            assert main_processor_id, "Processor not defined"
            last_timestamp = await session.scalar(select(EventProcessorGlobalStatus.last_event_ts).filter_by(id=main_processor_id))
        while last_timestamp is None or last_timestamp < latest_timestamp:
            event_query = select(Event).order_by(Event.created).limit(1)
            if last_timestamp is not None:
                event_query = event_query.filter(Event.created > last_timestamp)
            event = await session.execute(event_query.options(
                joinedload(Event.event_type).joinedload(Term.schema),
                joinedload(Event.source),
                joinedload(Event.creator),
                subqueryload(Event.handlers).joinedload(EventHandler.target_range)))
            event = event.one_or_none()
            if not event:
                return
            (event,) = event
            try:
                await applyEvent(session, event)
            except Exception as e:
                log.exception(e)
                raise e
            # Mark the event as processed
            last_timestamp = event.created
            await session.execute(
                insert(EventProcessorGlobalStatus
                    ).values(id=main_processor_id, last_event_ts=last_timestamp
                    ).on_conflict_do_update(
                        constraint='event_processor_global_status_pkey',
                        set_=dict(last_event_ts=last_timestamp)
                    ))
            await session.commit()


async def main(portal):
    global Session
    # TODO: Replace with the engine's connection, maybe?
    listener = asyncpg_listen.NotificationListener(
        asyncpg_listen.connect_func(
            user=config.get(target_db, 'owner'),
            password=config.get(target_db, 'owner_password'),
            host=config.get('postgres', 'host'),
            port=config.get('postgres', 'port'),
            database=config.get(target_db, 'database')))
    # TODO Use anyio?
    Session = make_scoped_session(asyncio.current_task)
    listener_task = asyncio.create_task(
        listener.run(
            {"events": handle_notifications},
            policy=asyncpg_listen.ListenPolicy.LAST,
            notification_timeout=5
        )
    )

    await portal.sleep_until_stopped()
    listener_task.cancel()


def work():
    global PORTAL_
    with anyio.from_thread.start_blocking_portal() as portal:
        PORTAL_ = portal
        portal.call(main, portal)

def start_listen_thread():
    t = Thread(target=work, daemon=True)
    t.start()

async def stop_listen_thread():
    global PORTAL_
    await PORTAL_.stop()
