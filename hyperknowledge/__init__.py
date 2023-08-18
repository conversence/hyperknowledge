from configparser import ConfigParser
from pathlib import Path
import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, async_scoped_session


config = ConfigParser()
# print(Path(__file__).parent.joinpath("config.ini"))
config.read(Path(__file__).parent.parent.joinpath("config.ini"))
production = os.environ.get("PRODUCTION", False)
target_db = os.environ.get("TARGET_DB", "production" if production else "development")


def db_config_get(key: str):
    return config.get(target_db, key)


db_name = db_config_get("database")

def engine_url(db=target_db, owner=True):
    user = 'owner' if owner else 'client'
    return f"postgresql+asyncpg://{config.get(db, user)}:{config.get(db, f'{user}_password')}@{config.get('postgres', 'host')}:{config.get('postgres', 'port')}/{db_name}"

def make_engine(db=target_db, owner=True):
    return create_async_engine(engine_url(db, owner))

OWNER_ENGINE_ = make_engine()
CLIENT_ENGINE_ = make_engine(owner=False)

def make_session(engine=None, owner=True):
    global OWNER_ENGINE_, CLIENT_ENGINE_
    return async_sessionmaker(engine or (OWNER_ENGINE_ if owner else CLIENT_ENGINE_), expire_on_commit=False)

Session = make_session()
ClientSession = make_session(owner=False)

def make_scoped_session(scope_func, owner=True):
    # Obscure bug: We need to recreate engine in this case
    # May be related to https://github.com/sqlalchemy/sqlalchemy/issues/6409 (closed?)
    return async_scoped_session(make_session(make_engine(owner=owner)), scopefunc=scope_func)
