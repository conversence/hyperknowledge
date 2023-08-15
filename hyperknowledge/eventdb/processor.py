"""Consume the events and update the projections

This happens on a thread, with attendant machinery.
"""
import asyncio
from threading import Thread
from datetime import datetime
from typing import Dict, Optional
from enum import Enum, IntEnum
import logging
from time import sleep
import asyncio

import anyio
import asyncpg_listen
from pydantic import Json, BaseModel
from sqlalchemy import select, update
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.functions import func
from py_mini_racer import MiniRacer

from hyperknowledge.eventdb.models import EventProcessor, Source

from .. import target_db, config, make_scoped_session
from . import dbTopicId
from .models import (
    Base, EventProcessor, Event, Source, Term, Topic, EventHandler
)
from .schemas import getEventModel, HkSchema, getProjectionSchemas, ProjectionSchema, GenericEventModel
from .make_tables import KNOWN_DB_MODELS, db_to_projection, projection_to_db

log = logging.getLogger("processor")

class QueuePosition(Enum):
    start = 'start'
    current = 'current'


class lifecycle(IntEnum):
    before = 0
    started = 1
    active = 2
    cancelling = 3
    cancelled = 4


class AbstractProcessorQueue():
    """A cache for event processors.
    Should know about last known event and last fetched event.
    If they correspond, the cache is caught up.
    """
    def __init__(self, proc: EventProcessor = None, queue_size=10) -> None:
        super(AbstractProcessorQueue, self).__init__()
        self.proc = proc
        self.source = proc.source if proc else None
        self.max_size = queue_size
        self.queue = asyncio.Queue(queue_size)
        self.in_process: Optional[Event] = None
        self.last_processed: Optional[datetime] = None
        self.last_fetched: Optional[datetime] = None
        self.last_seen: Optional[datetime] = None
        self.started = False
        self.status = lifecycle.before

    def add_event(self, event: Event) -> None:
        if self.started and (self.last_fetched is None or self.last_fetched == self.last_seen) and not self.queue.full():
            self.queue.put_nowait(event)
        self.set_last_seen(event.created)

    def set_last_seen(self, event_ts: datetime) -> None:
        self.last_seen = max(event_ts, self.last_seen or event_ts)

    async def fetch_events(self):
        async with self.session_maker() as session:
            if not self.started:
                max_date_q = select(func.max(Event.created))
                if self.source:
                    max_date_q = self.source.filter_event_query(max_date_q, session)
                self.last_seen = await session.scalar(max_date_q)
                if self.proc:
                    self.last_processed = EventProcessor.last_event_ts
            event_query = select(Event).order_by(Event.created).limit(self.max_size - self.queue.qsize())
            if self.last_processed is not None:
                event_query = event_query.filter(Event.created > self.last_processed).order_by(Event.created)
            if self.source:
                event_query = self.source.filter_event_query(event_query, session)
            events = await session.execute(event_query.options(
                joinedload(Event.event_type).joinedload(Term.schema),
                joinedload(Event.source),
                joinedload(Event.creator)))
            for (event,) in events:
                self.queue.put_nowait(event)
                self.last_fetched = event.created
            if self.last_seen is None and self.last_fetched is not None:
                log.warning("Event happened in between")
                # Refetch?
                self.last_seen = self.last_fetched
        self.started = True

    async def get_event(self) -> Event:
        if self.in_process:
            # Not yet acknowledged
            return self.in_process
        if not self.started or (
            # Catching up, empty queue
            self.queue.qsize() == 0 and self.last_seen is not None and (
                self.last_fetched is None or self.last_fetched < self.last_seen)
        ):
            await self.fetch_events()
        # Caught up
        self.in_process =  await self.queue.get()
        return self.in_process

    async def ack_event(self, event: Event, session=None):
        if session is None and self.proc:
            async with self.session_maker() as session:
                await self.ack_event(event, session)
                session.commit()
                return
        assert event == self.in_process
        last_timestamp = event.created
        self.last_processed = last_timestamp
        if self.proc:
            self.proc.last_event_ts = last_timestamp
            await session.execute(
                update(EventProcessor).where(EventProcessor.id==self.proc.id).values(last_event_ts=last_timestamp))
        self.queue.task_done()
        self.in_process = None

    def start_processor(self, tg: anyio.abc.TaskGroup, session_maker):
        self.tg = tg
        self.session_maker = session_maker
        self.status = lifecycle.started

    def stop_processor(self):
        self.status = lifecycle.cancelling



class PushProcessorQueue(AbstractProcessorQueue):

    async def start_processor(self, tg: anyio.abc.TaskGroup, session_maker):
        super(PushProcessorQueue, self).start_processor(tg, session_maker)
        tg.start_soon(self.run)

    async def run(self):
        while self.status < lifecycle.cancelling and not self.tg.cancel_scope.cancel_called:
            event = await self.get_event()
            async with self.session_maker() as session:
                await self.process_event(event, session)
                await self.ack_event(event, session)
                await session.commit()
        self.status = lifecycle.cancelled

    async def process_event(self, db_event: Event, session):
        pass


class ProjectionProcessor(PushProcessorQueue):
    def __init__(self, proc: EventProcessor = None, queue_size=10):
        super(ProjectionProcessor, self).__init__(proc=proc, queue_size=queue_size)
        self.js_ctx = MiniRacer()
        self.loaded_handlers = set()

    def forget_handler(self, handler_id: int):
        self.loaded_handlers.discard(handler_id)

    async def process_event(self, db_event: Event, session):
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
        # TODO: Do the following for each source that the event applies to
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
        # TODO: Cache
        handlers = await session.execute(select(EventHandler).filter_by(event_type_id=db_event.event_type_id).options(joinedload(EventHandler.target_range)))
        for (handler, ) in handlers:
            if handler.language != 'javascript':
                raise NotImplementedError()
            handler_fname = f"handler_{handler.id}"
            if handler.id not in self.loaded_handlers:
                handler_txt = handler.code_text.replace('function handler(', f'function {handler_fname}(')
                self.js_ctx.eval(handler_txt)
                self.loaded_handlers.add(handler.id)
            js = f"{handler_fname}({event.model_dump_json(by_alias=True, exclude_unset=True)}, {projection_by_role})"
            result = self.js_ctx.execute(js)
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


class Dispatcher(AbstractProcessorQueue, Thread):
    dispatcher = None
    def __init__(self):
        super(Dispatcher, self).__init__()
        assert Dispatcher.dispatcher is None, "Singleton"
        Dispatcher.dispatcher = self
        self.daemon = True
        self.status = lifecycle.before
        self.active_processors: Dict[int, AbstractProcessorQueue] = {}
        self.projection_processor: ProjectionProcessor = None

    def set_status(self, status: lifecycle):
        log.debug("Dispatcher status:", status)
        assert status >= self.status or (self.status == lifecycle.active and status == lifecycle.started)
        self.status = status

    def run(self) -> None:
        # Running in thread
        anyio.run(self.main_task)


    def add_processor(self, proc: AbstractProcessorQueue):
        assert self.status >= lifecycle.started
        assert proc.proc
        assert proc.proc.id not in self.active_processors
        self.active_processors[proc.proc.id] = proc
        proc.start_processor(self.tg, self.session_maker)

    def remove_processors(self, proc: AbstractProcessorQueue):
        assert proc.proc
        assert proc.proc.id in self.active_processors
        processor = self.active_processors.pop(proc.proc.id)
        processor.stop_processor()

    async def main_task(self) -> None:
        # Running in thread's event loop
        # TODO: Replace with the engine's connection, maybe?
        self.listener = asyncpg_listen.NotificationListener(
            asyncpg_listen.connect_func(
                user=config.get(target_db, 'owner'),
                password=config.get(target_db, 'owner_password'),
                host=config.get('postgres', 'host'),
                port=config.get('postgres', 'port'),
                database=config.get(target_db, 'database')))
        async with anyio.create_task_group() as tg:
            self.tg = tg
            self.session_maker = make_scoped_session(asyncio.current_task)
            self.set_status(lifecycle.started)
            await self.setup_projection_processor()
            tg.start_soon(self.do_listen)

        self.set_status(lifecycle.cancelled)


    async def setup_projection_processor(self):
        async with self.session_maker() as session:
            projection_processor = await session.get(EventProcessor, 0)
            await session.refresh(projection_processor, ['source'])
        self.projection_processor = ProjectionProcessor(projection_processor)
        self.tg.start_soon(self.projection_processor.start_processor, self.tg, self.session_maker)

    async def do_listen(self):
        await self.listener.run(
                {"events": self.handle_notifications},
                policy=asyncpg_listen.ListenPolicy.LAST,
                notification_timeout=5
                )
        await anyio.sleep_forever()

    def stop_task(self):
        assert self.status >= lifecycle.started
        was_active = self.status == lifecycle.active
        self.set_status(lifecycle.cancelling)
        if was_active:
            sleep(0.1)
        self.tg.cancel_scope.cancel()
        self.set_status(lifecycle.cancelled)

    async def handle_notifications(self, notification: asyncpg_listen.NotificationOrTimeout) -> None:
        if self.status >= lifecycle.cancelling or isinstance(notification, asyncpg_listen.Timeout):
            return
        self.set_status(lifecycle.active)
        latest_timestamp = datetime.fromisoformat(f'{notification.payload}00000'[:26])
        self.set_last_seen(latest_timestamp)
        # fetch events on everyone's behalf
        while self.status < lifecycle.cancelling and (self.queue.qsize() > 0 or self.last_fetched is None or self.last_fetched < self.last_seen):
            event = await self.get_event()
            # Do the projections first
            self.projection_processor.add_event(event)
            # Find a way to wait until the processing is done!
            # Then dispatch to other processors
            for processor in self.active_processors.values():
                await processor.add_event(event)
            await self.ack_event(event)
        self.set_status(lifecycle.started)


def start_listen_thread():
    p = Dispatcher()
    p.start()

def stop_listen_thread():
    assert Dispatcher.dispatcher, "Not started!"
    Dispatcher.dispatcher.stop_task()

def forget_handler(handler_id: int):
    assert Dispatcher.dispatcher, "Not started!"
    Dispatcher.dispatcher.forget_handler(handler_id)
