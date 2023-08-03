import os
from typing import Dict, Iterator
from httpx import AsyncClient
from asgi_lifespan import LifespanManager


import pytest

@pytest.fixture(scope="session")
async def sqla_engine(ini_file):
    os.environ["TARGET_DB"] = 'test'
    from hyperknowledge import make_engine
    engine = make_engine()

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def init_database(sqla_engine, ini_file):
    import sys
    sys.path.append("./scripts")
    from db_updater import get_connection_data, db_state, read_structure, init_db, deploy, revert
    db = 'test'
    conn_data = get_connection_data(ini_file, db)
    admin_conn_data = get_connection_data(ini_file, db, False, True)
    init_db(conn_data)
    structures = read_structure()
    deploy(
        None,
        db_state(conn_data),
        structures,
        conn_data,
        admin_conn_data=admin_conn_data
    )
    yield
    state = db_state(conn_data)
    revert(
        structures,
        db_state(conn_data),
        conn_data,
        admin_conn_data=admin_conn_data,
    )


@pytest.fixture()
async def session_maker(sqla_engine):
    """
    Fixture that returns a SQLAlchemy session_maker
    """
    from hyperknowledge import make_session
    return make_session()


@pytest.fixture()
async def clean_tables(session_maker):
    from hyperknowledge.eventdb.models import delete_data
    async with session_maker() as session:
        await delete_data(session)
    yield session_maker
    async with session_maker() as session:
        await delete_data(session)


@pytest.fixture(scope="module")
def app(init_database):
    from hyperknowledge.eventdb.server import app
    return app


@pytest.fixture(scope="module")
async def client(app) -> Iterator[AsyncClient]:
    async with LifespanManager(app):
        async with AsyncClient(app=app, base_url="http://test") as ac:
            yield ac


@pytest.fixture
def hk_events_ctx() -> Dict:
    from json import load
    with open("test/schemas/hyperknowledge_events.jsonld") as f:
        return load(f)

@pytest.fixture
def simple_schema() -> Dict:
    from json import load
    with open("test/schemas/simple_schema.json") as f:
        return load(f)

@pytest.fixture
def handlers() -> Dict:
    from yaml import safe_load
    with open("test/schemas/handlers.yaml") as f:
        return safe_load(f)
