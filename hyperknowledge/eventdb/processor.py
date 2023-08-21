"""Consume the events and update the projections

This happens on a thread, with attendant machinery.
"""
from __future__ import annotations

import asyncio
from threading import Thread
from datetime import datetime
from typing import Dict, Optional, Iterable, Union, Tuple
from collections import defaultdict
from enum import Enum, IntEnum
import logging
from time import sleep
import asyncio
from json import JSONDecodeError

import anyio
from anyio.from_thread import start_blocking_portal, BlockingPortal
import asyncpg_listen
from pydantic import Json, BaseModel
from sqlalchemy import select, update, delete
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.functions import func
from py_mini_racer import MiniRacer
from fastapi.websockets import WebSocket, WebSocketState
from starlette.websockets import WebSocketDisconnect

from .. import config, make_scoped_session, db_config_get, Session
from . import dbTopicId
from .models import (
    Base, EventProcessor, Event, Source, Term, Topic, EventHandler
)
from .schemas import getEventModel, HkSchema, getProjectionSchemas, ProjectionSchema, GenericEventModel, AgentModel
from .make_tables import KNOWN_DB_MODELS, db_to_projection, projection_to_db

log = logging.getLogger("processor")

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
                event_query = event_query.filter(Event.created > (self.last_fetched or self.last_processed)).order_by(Event.created)
            if self.source:
                event_query = self.source.filter_event_query(event_query, session)
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
            async with self.session_maker() as session:
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

    def start_processor(self, tg: anyio.abc.TaskGroup, session_maker):
        self.tg = tg
        self.session_maker = session_maker
        self.status = lifecycle.started

    def stop_processor(self):
        self.status = lifecycle.cancelling



class PushProcessorQueue(AbstractProcessorQueue):

    def __init__(self, proc: EventProcessor = None, queue_size=10, autoack=True) -> None:
        super().__init__(proc, queue_size)
        self.autoack = autoack
        self.ack_wait_event = None

    async def start_processor(self, tg: anyio.abc.TaskGroup, session_maker):
        super(PushProcessorQueue, self).start_processor(tg, session_maker)
        tg.start_soon(self.run)

    async def ack_event(self, event: Event, session=None):
        await super().ack_event(event, session)
        if session is not None and not self.autoack:
            self.ack_wait_event.set()

    async def run(self):
        while self.status < lifecycle.cancelling and not self.tg.cancel_scope.cancel_called:
            event = await self.get_event()
            async with self.session_maker() as session:
                await self.process_event(event, session)
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

    def after_event(self, db_event: Event):
        WebSocketDispatcher.dispatcher.add_event(db_event)


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
        log.debug("Dispatcher status:", status)
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
                user=db_config_get('owner'),
                password=db_config_get('owner_password'),
                host=config.get('postgres', 'host'),
                port=config.get('postgres', 'port'),
                database=db_config_get('database')))
        async with BlockingPortal() as portal:
            self.portal = portal
            async with anyio.create_task_group() as tg:
                self.tg = tg
                self.session_maker = make_scoped_session(asyncio.current_task)
                self.set_status(lifecycle.started)
                await self.setup_main_processors()
                tg.start_soon(self.do_listen)

        self.set_status(lifecycle.cancelled)


    async def setup_main_processors(self):
        async with self.session_maker() as session:
            projection_processor = await session.get(EventProcessor, 0)
            await session.refresh(projection_processor, ['source'])
        self.projection_processor = ProjectionProcessor(projection_processor)
        self.tg.start_soon(self.projection_processor.start_processor, self.tg, self.session_maker)
        self.websocket_processor = WebSocketDispatcher()
        self.tg.start_soon(self.websocket_processor.start_processor, self.tg, self.session_maker)
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


class WebSocketDispatcher(PushProcessorQueue):
    """This dispatches events to all websocket clients"""
    dispatcher: WebSocketDispatcher = None

    def __init__(self) -> None:
        super(WebSocketDispatcher, self).__init__()
        assert not WebSocketDispatcher.dispatcher, "Singleton"
        self.sockets: Dict[str, WebSocketHandler] = {}
        self.by_source: Dict[int, Dict[Tuple[int, str], WebSocketProcessor]] = defaultdict(dict)
        WebSocketDispatcher.dispatcher = self

    def add_socket(self, socket: WebSocket, agent: AgentModel):
        handler = WebSocketHandler(socket, agent)
        self.sockets[handler.name] = handler
        return handler

    def close_socket(self, handler: WebSocketHandler):
        for proc in handler.processors.values():
            self.remove_ws_processor(proc)
        del self.sockets[handler.name]

    async def fetch_events(self):
        # The dispatcher should not fetch events on its own
        self.started = True

    def add_ws_processor(self, wproc: WebSocketProcessor):
        for source_id in wproc.source_set:
            assert not self.by_source[source_id].get((wproc.proc.owner_id, wproc.proc.name)), "Do not listen to a single processor from two sockets"
            self.by_source[source_id][(wproc.proc.owner_id, wproc.proc.name)] = wproc
        self.tg.start_soon(wproc.start_processor, self.tg, self.session_maker)

    def remove_ws_processor(self, wproc: WebSocketProcessor):
        for source_id in wproc.source_set:
            wproc = self.by_source[source_id].pop((wproc.proc.owner_id, wproc.proc.name))
        wproc.stop_processor()

    async def process_event(self, event: Event, session=None):
        #TODO multisource
        sources = [event.source_id]
        for source_id in sources:
            for processor in self.by_source[source_id].values():
                processor.add_event(event)

    def stop_processor(self):
        for handler in self.sockets.values():
            self.tg.start_soon(handler.close)
        super().stop_processor()


class WebSocketProcessor(PushProcessorQueue):
    """Processors keep track of position in the event stream for each source.
    This processor sends events to a websocket connection as they are made available."""
    def __init__(self, socket: WebSocket, proc: EventProcessor, source_set:Iterable[int], autoack=True):
        super(WebSocketProcessor, self).__init__(proc=proc, autoack=autoack)
        self.socket = socket
        self.proc = proc
        self.source_set = source_set

    async def process_event(self, event: Event, session):
        ev_class = getEventModel()
        event_data = ev_class.model_validate(event)
        # What about context?
        # Here we're on the processor thread, ideally we'd want this to run in main event loop that has the actual socket
        # main_portal.start_task_soon(self.socket.send_text, event_data.model_dump_json())
        await self.socket.send_text(event_data.model_dump_json())

    # async def fetch_events(self):
    #     # Big problem: Which past events have been processed?
    #     # Temporary disable for debugging
    #     self.started = True
    #     return

class WebSocketHandler():
    """Handles a single websocket connection and the associated processors"""
    def __init__(self, socket: WebSocket, agent: AgentModel):
        self.socket = socket
        self.name = socket._headers['sec-websocket-key']
        self.agent = agent
        self.processors: Dict[str, WebSocketProcessor] = {}

    @staticmethod
    def set_start_time(proc: EventProcessor, start_time: Union[QueuePosition, datetime]=QueuePosition.current):
        if start_time == QueuePosition.current:
            return
        elif start_time == QueuePosition.last:
            proc.last_event_ts = datetime.utcnow()
        elif start_time == QueuePosition.start:
            proc.last_event_ts = None
        else:
            proc.last_event_ts = start_time


    async def listen(self, source_name: str, proc_name: Optional[str]=None, start_time:Union[QueuePosition, datetime]=QueuePosition.current, autoack=True) -> WebSocketProcessor:
        assert proc_name or source_name
        if proc_name in self.processors:
            return
        async with Session() as session:
            q = select(EventProcessor).filter_by(owner_id=self.agent.id)
            if proc_name:
                q = q.filter_by(name=proc_name)
            if source_name:
                q = q.join(Source).filter_by(local_name=source_name)
            q = q.options(joinedload(EventProcessor.source))
            r = await session.execute(q)
            if r := r.first():
                (processor,) = r
            elif source_name:
                source = await session.execute(select(Source).filter_by(local_name=source_name))
                source = source.first()
                if not source:
                    raise RuntimeError(f"No such source {source_name}")
                (source,) = source
                processor = EventProcessor(source=source, owner_id=self.agent.id, name=proc_name or source_name)
                session.add(processor)
            else:
                raise RuntimeError(f"No such processor {proc_name}")
            self.set_start_time(processor, start_time)
            # TODO
            source_set = {processor.source.id}
            await session.commit()
        wproc = WebSocketProcessor(self.socket, processor, source_set, autoack=autoack)
        self.processors[proc_name] = wproc
        # We're in the main thread; we want to run in the processor thread
        Dispatcher.dispatcher.portal.start_task_soon(WebSocketDispatcher.dispatcher.add_ws_processor, wproc)
        return wproc

    async def mute(self, proc_name: str):
        proc = self.processors.pop(proc_name)
        assert proc
        WebSocketDispatcher.dispatcher.remove_ws_processor(proc)

    async def delete_processor(self, proc_name: str):
        await self.mute(proc_name)
        async with Session() as session:
            r = await session.execute(
                delete(EventProcessor).where(owner_id=self.agent.id, name=proc_name))
            session.commit()

    async def close(self):
        for proc_name in self.processors:
            await self.mute(proc_name)
        self.socket.close()

    async def ack_event(self, proc_name: str, event_time: Optional[datetime] = None) -> datetime:
        # We're in the main thread; we want to run in the processor thread
        processor = self.processors[proc_name]
        assert not processor.autoack
        current_event = processor.in_process
        assert current_event
        Dispatcher.dispatcher.portal.start_task_soon(self.ack_event_2, processor, event_time or current_event.created)
        return current_event.created

    async def ack_event_2(self, processor: EventProcessor, event_time: Optional[datetime]):
        assert not processor.autoack
        await processor.ack_event_by_time(event_time)

    async def handle_ws(self):
        alive = True
        ws = self.socket
        try:
            while alive:
                try:
                    cmd_data = await ws.receive_json()
                except JSONDecodeError as e:
                    await ws.send_json(dict(error=f"Invalid JSON {e.msg}"))
                    continue
                base_cmd = cmd_data.get('cmd')
                if not base_cmd:
                    await ws.send_json(dict(error="Please provide cmd"))
                    continue
                if base_cmd == 'listen':
                    source = cmd_data.get('source')
                    processor = cmd_data.get('processor')
                    autoack = cmd_data.get('autoack', True)
                    if not (source or processor):
                        await ws.send_json(dict(error="Include source or processor"))
                    start_time = cmd_data.get('start') or "current"
                    if start_time in QueuePosition._member_map_:
                        start_time = QueuePosition._member_map_[start_time]
                    else:
                        try:
                            start_time = datetime.fromisoformat(start_time)
                        except ValueError:
                            await ws.send_json(dict(error=f"Invalid start_time: Must be a iso-formatted date or in {QueuePosition._member_names_}."))
                            continue
                    try:
                        await self.listen(source, processor, start_time, autoack=autoack)
                        await ws.send_json(dict(listen=processor or source, autoack=autoack))
                    except AssertionError:
                        if source:
                            await ws.send_json(dict(error=f"No such source {source}"))
                        else:
                            await ws.send_json(dict(error=f"No such processor {processor}"))
                elif base_cmd == 'mute':
                    processor = cmd_data.get('processor')
                    if not processor:
                        await ws.send_json(dict(error="Specify processor"))
                        continue
                    try:
                        await self.mute(processor)
                        await ws.send_json(dict(mute=processor))
                    except KeyError:
                        await ws.send_json(dict(error=f"No such processor {processor}"))
                elif base_cmd == 'delete':
                    processor = cmd_data.get('processor')
                    if not processor:
                        await ws.send_json(dict(error="Specify processor"))
                        continue
                    try:
                        await self.delete(processor)
                        await ws.send_json(dict(delete=processor))
                    except KeyError:
                        await ws.send_json(dict(error=f"No such processor {processor}"))
                elif base_cmd == 'ack':
                    processor = cmd_data.get('processor')
                    if not processor:
                        await ws.send_json(dict(error="Specify processor"))
                        continue
                    if event_time := cmd_data.get('event_time', None):
                        try:
                            event_time = datetime.fromisoformat(event_time)
                        except ValueError:
                            await ws.send_json(dict(error="Invalid event_time: Must be a iso-formatted date"))
                            continue
                    try:
                        ev_time = await self.ack_event(processor, event_time)
                        await ws.send_json(dict(ack=ev_time.isoformat()))
                    except KeyError:
                        await ws.send_json(dict(error=f"No such processor {processor}"))
                elif base_cmd == 'close':
                    alive = False
                else:
                    await ws.send_json(dict(error="Unknown command"))
        except WebSocketDisconnect:
            pass
        except Exception as e:
            log.exception(e)
        finally:
            exc = None
            if ws.client_state != WebSocketState.DISCONNECTED:
                try:
                    await ws.close()
                except Exception as e:
                    exc = e
            WebSocketDispatcher.dispatcher.close_socket(self)
            if exc is not None:
                raise exc


def start_listen_thread():
    p = Dispatcher()
    p.start()

def stop_listen_thread():
    assert Dispatcher.dispatcher, "Not started!"
    Dispatcher.dispatcher.stop_task()

def forget_handler(handler_id: int):
    assert Dispatcher.dispatcher, "Not started!"
    Dispatcher.dispatcher.forget_handler(handler_id)
