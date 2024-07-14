from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from json import JSONDecodeError
from typing import Dict, Union, Optional, Iterable, Tuple
import logging

from sqlalchemy import select, delete
from sqlalchemy.orm import joinedload
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from . import owner_scoped_session
from .models import EventProcessor, Source, Event
from .processor import QueuePosition, Dispatcher, PushProcessorQueue
from .schemas import AgentModel, getEventModel

log = logging.getLogger("hyperknowledge.eventdb.websockets")

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
        self.tg.start_soon(wproc.start_processor, self.tg)

    def remove_ws_processor(self, wproc: WebSocketProcessor):
        for source_id in wproc.source_set:
            wproc = self.by_source[source_id].pop((wproc.proc.owner_id, wproc.proc.name))
        wproc.stop_processor()

    async def process_event(self, event: Event, session):
        e2 = await session.merge(event, load=False)
        await session.refresh(e2, ['including_source_ids_rec'])
        for source_id in e2.including_source_ids_rec:
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
        await self.socket.send_text(event_data.model_dump_json(by_alias=True))

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
            return self.processors[proc_name]
        # sessionmaker?
        async with owner_scoped_session() as session:
            q = select(EventProcessor).filter_by(owner_id=self.agent.id)
            if proc_name:
                q = q.filter_by(name=proc_name)
            if source_name:
                q = q.join(Source).filter_by(local_name=source_name)
            q = q.options(joinedload(EventProcessor.source))
            r = await session.execute(q)
            if r := r.first():
                (processor,) = r
                source = processor.source
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
            await session.refresh(source, ['included_source_ids_rec'])
            await session.commit()
        wproc = WebSocketProcessor(self.socket, processor, source.included_source_ids_rec, autoack=autoack)
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
        async with owner_scoped_session() as session:
            await session.execute(
                delete(EventProcessor).where(owner_id=self.agent.id, name=proc_name))
            session.commit()

    async def close(self):
        for proc_name in self.processors:
            await self.mute(proc_name)
        await self.socket.close()

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
                elif base_cmd == 'ping':
                    await ws.send_json(dict(pong=True))
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
