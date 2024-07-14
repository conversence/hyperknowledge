from asyncio import current_task
from datetime import datetime
from threading import current_thread
from uuid import UUID

from pydantic import constr
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import BIGINT
from sqlalchemy.ext.asyncio import (
    async_scoped_session,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.util import ScopedRegistry
from sqlalchemy import event

from .pydantic_adapters import PydanticURIRef
from .. import target_db, engine_url, db_config_get

Name = constr(pattern=r"\w+")
QName = constr(pattern=r"\w+:\w+")

StreamId = PydanticURIRef
Timestamp = datetime
EntityId = UUID
SchemaId = PydanticURIRef
SchemaName = Name
SchemaElementFull = PydanticURIRef
SchemaElement = QName
UserId = Name
dbTopicId = BIGINT


def as_tuple(val):
    if isinstance(val, (tuple, list)):
        return tuple(val)
    return (val,)


def as_tuple_or_scalar(val):
    if isinstance(val, (tuple, list)):
        return tuple(val)
    return val


async def setup_connection_enums(conn):
    from .models import all_enums

    for e in all_enums:
        await conn.set_builtin_type_codec(
            str(e.name), schema="public", codec_name="text"
        )


def make_engine(db=target_db):
    engine = create_async_engine(engine_url(db, False))
    # cf https://github.com/MagicStack/asyncpg/issues/530#issuecomment-1424610433

    @event.listens_for(engine.sync_engine, "connect")
    def connect(conn, connection_record):
        conn.await_(setup_connection_enums(conn.driver_connection))

    return engine


class async_role_sessionmaker(async_sessionmaker):
    def __init__(self, *args, role=None, **kwargs):
        self.role = role
        super(async_role_sessionmaker, self).__init__(*args, **kwargs)

    def set_role(self, session):
        if self.role:
            session.execute(text(f"SET ROLE {self.role}"))

    def __call__(self, **local_kw):
        session = super(async_role_sessionmaker, self).__call__(**local_kw)

        @event.listens_for(session.sync_session, "after_transaction_create")
        def after_transaction_create(session, transaction):
            if transaction.parent is None:
                self.set_role(session)

        @event.listens_for(session.sync_session, "after_rollback")
        def after_rollback(session):
            self.set_role(session)

        return session


def make_session_factory(owner=True):
    role = db_config_get("owner" if owner else "client")
    return async_role_sessionmaker(make_engine(), expire_on_commit=False, role=role)


# We need an engine per thread, see https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#using-multiple-asyncio-event-loops
owner_session_factory_registry = ScopedRegistry(make_session_factory, current_thread)
client_session_factory_registry = ScopedRegistry(
    lambda: make_session_factory(owner=False), current_thread
)


owner_scoped_session = async_scoped_session(
    lambda: owner_session_factory_registry()(), scopefunc=current_task
)
client_scoped_session = async_scoped_session(
    lambda: client_session_factory_registry()(), scopefunc=current_task
)
