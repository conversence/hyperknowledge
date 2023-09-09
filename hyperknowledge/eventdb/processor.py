"""Consume the events and update the projections

This happens on a thread, with attendant machinery.
"""
from __future__ import annotations

from threading import Thread
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List
from itertools import chain
from enum import Enum, IntEnum
import logging
from time import sleep
import asyncio
from json import dumps

import anyio
from anyio.from_thread import BlockingPortal
import asyncpg_listen
from pydantic import Json, BaseModel
from sqlalchemy import select, update
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.functions import func
from py_mini_racer import MiniRacer

from .. import config, db_config_get, production
from . import dbTopicId, as_tuple, owner_scoped_session
from .models import (
    Base, EventProcessor, Event, Term, Topic, EventHandler, UUIDentifier
)
from .schemas import (
  getEventModel, HkSchema, getProjectionSchemas, ProjectionSchema, EventAttributeSchema, HistoryStorageDirective)
from .make_tables import KNOWN_DB_MODELS, db_to_projection, projection_to_db, KNOWN_SNAPSHOT_MODELS

log = logging.getLogger("hyperknowledge.eventdb.processor")

# TODO: Parametrize
snapshot_interval = timedelta(minutes=1)

class QueuePosition(Enum):
    start = 'start'
    current = 'current'
    last = 'last'


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
        self.last_processed: Optional[datetime] = proc.last_event_ts if proc else None
        self.last_fetched: Optional[datetime] = None
        self.last_seen: Optional[datetime] = None
        self.started = False
        self.status = lifecycle.before

    def add_event(self, event: Event) -> None:
        if self.started and (self.last_fetched is None or self.last_fetched == self.last_seen) and not self.queue.full():
            self.queue.put_nowait(event)
            self.last_fetched = event.created
        self.set_last_seen(event.created)

    def set_last_seen(self, event_ts: datetime) -> None:
        self.last_seen = max(event_ts, self.last_seen or event_ts)

    async def fetch_events(self):
        async with owner_scoped_session() as session:
            if not self.started:
                max_date_q = select(func.max(Event.created))
                if self.source:
                    max_date_q = self.source.filter_event_query(max_date_q)
                self.last_seen = await session.scalar(max_date_q)
                if self.proc:
                    self.last_processed = EventProcessor.last_event_ts
            event_query = select(Event).order_by(Event.created).limit(self.max_size - self.queue.qsize())
            if self.last_processed is not None:
                event_query = event_query.filter(Event.created > (self.last_fetched or self.last_processed)).order_by(Event.created)
            if self.source:
                event_query = self.source.filter_event_query(event_query)
            events = await session.execute(event_query.options(
                joinedload(Event.event_type).joinedload(Term.schema),
                joinedload(Event.source),
                joinedload(Event.creator)))
            for (event,) in events:
                await self.queue.put(event)
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
            async with owner_scoped_session() as session:
                await self.ack_event(event, session)
                await session.commit()
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

    async def ack_event_by_time(self, event_time: Optional[datetime] = None):
        if event_time:
            assert event_time == self.in_process.created
        await self.ack_event(self.in_process)

    def start_processor(self, tg: anyio.abc.TaskGroup):
        self.tg = tg
        self.status = lifecycle.started

    def stop_processor(self):
        self.status = lifecycle.cancelling



class PushProcessorQueue(AbstractProcessorQueue):

    def __init__(self, proc: EventProcessor = None, queue_size=10, autoack=True) -> None:
        super().__init__(proc, queue_size)
        self.autoack = autoack
        self.ack_wait_event = None

    async def start_processor(self, tg: anyio.abc.TaskGroup):
        super(PushProcessorQueue, self).start_processor(tg)
        tg.start_soon(self.run)

    async def ack_event(self, event: Event, session=None):
        await super().ack_event(event, session)
        if session is not None and not self.autoack:
            self.ack_wait_event.set()

    async def run(self):
        while self.status < lifecycle.cancelling and not self.tg.cancel_scope.cancel_called:
            event = await self.get_event()
            async with owner_scoped_session() as session:
                try:
                    await self.process_event(event, session)
                except Exception as e:
                    log.exception(e)
                    if not production:
                        import pdb
                        pdb.post_mortem()
                if self.autoack:
                    await self.ack_event(event, session)
                    await session.commit()
                else:
                    await session.commit()
                    await self.ack_wait_event.wait()
            self.after_event(event)
        self.status = lifecycle.cancelled

    async def get_event(self) -> Event:
        event = await super().get_event()
        if not self.autoack:
            self.ack_wait_event = anyio.Event()
        return event

    def after_event(self, deb_event: Event):
        pass

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
        projection_schemas = getProjectionSchemas()
        range_schemas_by_role: Dict[str, List[Tuple[ProjectionSchema, BaseModel, type[Base]]]] = {}
        topic_by_role: Dict[str, Topic] = {}
        attrib_schema_by_name: Dict[str, EventAttributeSchema] = {}
        for attrib_schema in event_hk_schema.attributes:
            attrib_schema_by_name[attrib_schema.name] = attrib_schema
            value = getattr(event.data, attrib_schema.name)
            if not value:
                continue
            projection_data: List[Tuple[ProjectionSchema, BaseModel, type[Base]]] = []
            for range in as_tuple(attrib_schema.range):
                range_schema, range_model = projection_schemas.get(range, (None, None))
                if not range_schema:
                    continue
                range_db_model = KNOWN_DB_MODELS.get(range_schema.type)
                if not range_db_model:
                    log.warning("Missing model! %s", range_schema.type)
                    continue
                projection_data.append((range_schema, range_model, range_db_model))
            if projection_data:
                range_schemas_by_role[attrib_schema.name] = projection_data
            else:
                continue
            entity_id = await Topic.get_by_uri(session, value)
            if not entity_id:
                if attrib_schema.create and value.startswith('urn:uuid:'):
                    entity_id = await UUIDentifier.ensure_url(session, value)
                else:
                    log.warning("Missing topic: %s", value)
                    continue
            topic_by_role[attrib_schema.name] = entity_id
        range_urls = set(chain(*[[r[0].type for r in v] for v in  range_schemas_by_role.values()]))
        ranges: Dict[str, Term] = {}
        for url in range_urls:
            ranges[url] = await Term.get_by_uri(session, url)
        # TODO: Cache handlers, make the handlers per source
        # TODO: Maybe only those handlers with a target in the known terms?
        r = await session.execute(select(EventHandler).filter_by(event_type_id=db_event.event_type_id).options(joinedload(EventHandler.target_range)))
        handlers: List[EventHandler] = r.scalars().all()
        # Note the event may come from another session
        db_event2 = await session.merge(db_event)
        await session.refresh(db_event2, ("included_in_sources",))
        for source in db_event2.included_in_sources:
            db_object_by_role: Dict[str, Base] = {}
            projection_by_role: Dict[str, Json] = {}
            updated_projection_by_role: Dict[str, Json] = {}
            used_projection_data_by_role: Dict[str, Tuple[ProjectionSchema, BaseModel, type[Base]]] = {}
            for attrib_schema in event_hk_schema.attributes:
                projection_data: List[Tuple[ProjectionSchema, BaseModel, type[Base]]] = range_schemas_by_role.get(attrib_schema.name)
                if not projection_data:
                    continue
                topic = topic_by_role.get(attrib_schema.name)
                if not topic:
                    if attrib_schema.create:
                        used_projection_data_by_role[attrib_schema.name] = projection_data[0]
                    continue
                # TODO: Check that range's id is in topic's as_projection
                for range_schema, range_model, range_db_model in projection_data:
                    # TODO: Use Topic.has_projections to avoid a few loops
                    db_projection = await session.execute(range_db_model.current_query().filter_by(id=entity_id.id, source_id=source.id))
                    if db_projection := db_projection.one_or_none():
                        # Note: Using first matching projection. What happens if there are many matches
                        used_projection_data_by_role[attrib_schema.name] = (range_schema, range_model, range_db_model)
                        db_projection = db_projection[0]
                        pd_projection = await db_to_projection(session, db_projection, range_model, range_schema)
                        db_object_by_role[attrib_schema.name] = db_projection
                        projection_by_role[attrib_schema.name] = pd_projection.model_dump(by_alias=True)
                        break
                else:
                    if attrib_schema.create:
                        used_projection_data_by_role[attrib_schema.name] = projection_data[0]
                    else:
                        log.warn(f"Missing projections for {attrib_schema} in {db_event}")
                        continue
            # apply the handlers
            # TODO: Maybe start with handlers that will create objects?
            for handler in handlers:
                if handler.language != 'javascript':
                    raise NotImplementedError()
                handler_fname = f"handler_{handler.id}"
                if handler.id not in self.loaded_handlers:
                    handler_txt = handler.code_text.replace('function handler(', f'function {handler_fname}(')
                    self.js_ctx.eval(handler_txt)
                    self.loaded_handlers.add(handler.id)
                target_attrib_schema = attrib_schema_by_name[handler.target_role]
                topic = topic_by_role[handler.target_role]
                if handler.target_role not in projection_by_role and not target_attrib_schema.create:
                    continue
                js = f"{handler_fname}({event.model_dump_json(by_alias=True, exclude_unset=True)}, {dumps(projection_by_role)})"
                result = self.js_ctx.execute(js)
                # Convenience so the handler does not have to do it
                if result["@type"] == event.data.type:
                    result["@type"] = hk_schema.context.shrink_iri(handler.target_range.uri)
                range_schema, range_model, range_db_model = used_projection_data_by_role[handler.target_role]
                new_projection = range_model.model_validate(result)
                db_data = await projection_to_db(session, new_projection, range_schema)
                do_create = False
                do_update = False
                do_transfer = False
                prev_state = db_object_by_role.get(handler.target_role)
                if prev_state is None:
                    # if no past state, just create
                    do_create = True
                elif range_schema.history_storage == HistoryStorageDirective.no_history:
                    # No history: always update
                    do_update = True
                elif range_schema.history_storage == HistoryStorageDirective.full_history:
                    # Full history: Always add a new state
                    do_create = True
                elif range_schema.history_storage == HistoryStorageDirective.separate_history:
                    # Separate history: Check how long since latest snapshot
                    snapshot_cls = KNOWN_SNAPSHOT_MODELS[range_schema.type]
                    previous_snapshot_time = await session.scalar(
                        select(snapshot_cls.when).filter_by(source_id=source.id, id=prev_state.id).order_by(snapshot_cls.when.desc()).limit(1)
                    )
                    if previous_snapshot_time is None or prev_state.when - previous_snapshot_time > snapshot_interval:
                        # If long enough, transfer old data to new.
                        do_transfer = True
                    do_update = True
                elif range_schema.history_storage == HistoryStorageDirective.mixed_history:
                    # Mixed history: Check how long since latest snapshot
                    previous_snapshot_time = await session.scalar(
                        select(range_db_model.when).filter_by(source_id=source.id, id=prev_state.id).filter(range_db_model.when < prev_state.when).order_by(snapshot_cls.when.desc()).limit(1)
                    )
                    if previous_snapshot_time is None or prev_state.when - previous_snapshot_time > snapshot_interval:
                        do_create = True
                    else:
                        do_update = True
                else:
                    raise RuntimeError("Unknown history_storage value")
                if do_transfer:
                    session.add(snapshot_cls(**{
                        c.name: getattr(prev_state, c.name)
                        for c in prev_state.__class__.__mapper__.columns
                    }))
                if do_update:
                    db_projection = db_object_by_role[handler.target_role]
                    for k, v in db_data.items():
                        setattr(db_projection, k, v)
                    db_projection.when = event.created
                if do_create:
                    db_projection = range_db_model(
                        id= topic_by_role[handler.target_role].id,source_id=source.id, when=db_event.created ,**db_data)
                    session.add(db_projection)
                # Not sure that this is useful. Certainly do not update current projections.
                updated_projection_by_role[handler.target_role] = new_projection.model_dump()
                # TODO: Make sure the projection type is in the topic
                range_term = ranges[range_schema.type]
                if range_term.id not in topic.has_projections:
                    topic.has_projections.append(range_term.id)
                    flag_modified(topic, 'has_projections')

    def after_event(self, db_event: Event):
        Dispatcher.dispatcher.websocket_processor.dispatcher.add_event(db_event)


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
        self.websocket_processor: PushProcessorQueue = None

    def set_status(self, status: lifecycle):
        log.debug("Dispatcher status: %s", status)
        assert status >= self.status or (self.status == lifecycle.active and status == lifecycle.started)
        self.status = status

    def run(self) -> None:
        # Running in thread
        anyio.run(self.main_task)


    def add_processor(self, proc: AbstractProcessorQueue):
        # assert self.status >= lifecycle.started
        assert proc.proc
        assert proc.proc.id not in self.active_processors
        self.active_processors[proc.proc.id] = proc
        proc.start_processor(self.tg)

    def remove_processors(self, proc: AbstractProcessorQueue):
        assert proc.proc
        assert proc.proc.id in self.active_processors
        processor = self.active_processors.pop(proc.proc.id)
        processor.stop_processor()

    async def main_task(self) -> None:
        # Running in thread's event loop
        task = asyncio.current_task()
        task.get_loop().name = 'dispatcher_loop'
        # TODO: Replace with the engine's connection, maybe?
        self.listener = asyncpg_listen.NotificationListener(
            asyncpg_listen.connect_func(
                user=db_config_get('owner'),
                password=db_config_get('owner_password'),
                host=config.get('postgres', 'host'),
                port=config.get('postgres', 'port'),
                database=db_config_get('database')))
        async with BlockingPortal() as portal:
            self.portal = portal
            async with anyio.create_task_group() as tg:
                self.tg = tg
                self.set_status(lifecycle.started)
                await self.setup_main_processors()
                tg.start_soon(self.do_listen)

        self.set_status(lifecycle.cancelled)


    async def setup_main_processors(self):
        from .websockets import WebSocketDispatcher

        async with owner_scoped_session() as session:
            projection_processor = await session.get(EventProcessor, 0)
            await session.refresh(projection_processor, ['source'])
        self.projection_processor = ProjectionProcessor(projection_processor)
        self.tg.start_soon(self.projection_processor.start_processor, self.tg)
        self.websocket_processor = WebSocketDispatcher()
        self.tg.start_soon(self.websocket_processor.start_processor, self.tg)
        self.last_processed = self.projection_processor.last_processed

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
        self.websocket_processor.stop_processor()
        self.projection_processor.stop_processor()
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
            # TODO: Maybe the post-projection processor should be a separate dispatcher instead of just the websocket dispatcher?
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
