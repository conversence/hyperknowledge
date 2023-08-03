from configparser import ConfigParser
from pathlib import Path
import os

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, async_scoped_session


config = ConfigParser()
# print(Path(__file__).parent.joinpath("config.ini"))
config.read(Path(__file__).parent.parent.joinpath("config.ini"))
production = os.environ.get("PRODUCTION", False)
target_db = os.environ.get("TARGET_DB", "production" if production else "development")
config.get(target_db, "database")

def engine_url(db=target_db):
    return f"postgresql+asyncpg://{config.get(db, 'owner')}:{config.get(db, 'owner_password')}@{config.get('postgres', 'host')}:{config.get('postgres', 'port')}/{config.get(db, 'database')}"

def make_engine(db=target_db):
    return create_async_engine(engine_url(db))

ENGINE_ = make_engine()

def make_session(engine=None):
    global ENGINE_
    return async_sessionmaker(engine or ENGINE_, expire_on_commit=False)

Session = make_session()

def make_scoped_session(scope_func):
    # Obscure bug: We need to recreate engine in this case
    # May be related to https://github.com/sqlalchemy/sqlalchemy/issues/6409 (closed?)
    return async_scoped_session(make_session(make_engine()), scopefunc=scope_func)
