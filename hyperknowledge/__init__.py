from configparser import ConfigParser
from pathlib import Path
from asyncio import current_task
from threading import current_thread
import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, async_scoped_session
from sqlalchemy.util import ScopedRegistry

config = ConfigParser()
# print(Path(__file__).parent.joinpath("config.ini"))
config.read(Path(__file__).parent.parent.joinpath("config.ini"))
production = os.environ.get("PRODUCTION", False)
target_db = os.environ.get("TARGET_DB", ("test" if "PYTEST_CURRENT_TEST" in os.environ else ("production" if production else "development")))


def db_config_get(key: str):
    return config.get(target_db, key)


db_name = db_config_get("database")

def engine_url(db=target_db, owner=True):
    user = 'owner' if owner else 'client'
    return f"postgresql+asyncpg://{config.get(db, user)}:{config.get(db, f'{user}_password')}@{config.get('postgres', 'host')}:{config.get('postgres', 'port')}/{db_name}"

def make_engine(db=target_db, owner=True):
    return create_async_engine(engine_url(db, owner))

def make_session_factory(owner=True):
    return async_sessionmaker(make_engine(owner=owner), expire_on_commit=False)

# We need an engine per thread, see https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html#using-multiple-asyncio-event-loops
owner_session_factory_registry = ScopedRegistry(make_session_factory, current_thread)
client_session_factory_registry = ScopedRegistry(lambda: make_session_factory(owner=False), current_thread)


owner_scoped_session = async_scoped_session(lambda: owner_session_factory_registry()(), scopefunc=current_task)
client_scoped_session = async_scoped_session(lambda: client_session_factory_registry()(), scopefunc=current_task)
