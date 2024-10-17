import os
from typing import Dict, Iterator
import logging

from httpx import AsyncClient
from httpx_ws.transport import ASGIWebSocketTransport
from asgi_lifespan import LifespanManager
from fastapi import FastAPI

from fastapi.security import OAuth2PasswordRequestForm

# We do not want to load hk here, because that would load the session.

import pytest


@pytest.fixture(scope="session")
def logger():
    return logging.getLogger("tests")


@pytest.fixture(scope="session")
async def sqla_engine(ini_file):
    os.environ["TARGET_DB"] = "test"
    from hyperknowledge.eventdb import make_engine

    engine = make_engine()

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def init_database(sqla_engine, ini_file):
    import sys

    sys.path.append("./scripts")
    from db_updater import (
        get_connection_data,
        db_state,
        read_structure,
        init_db,
        deploy,
        revert,
    )

    db = "test"
    conn_data = get_connection_data(ini_file, db)
    admin_conn_data = get_connection_data(ini_file, db, admin_password=True)
    init_db(conn_data)
    structures = read_structure()
    deploy(
        None,
        db_state(conn_data),
        structures,
        conn_data,
        admin_conn_data=admin_conn_data,
    )
    yield
    db_state(conn_data)
    revert(
        structures,
        db_state(conn_data),
        conn_data,
        admin_conn_data=admin_conn_data,
    )


@pytest.fixture(scope="function")
async def clean_tables():
    from hyperknowledge.eventdb import owner_scoped_session
    from hyperknowledge.eventdb.models import delete_data

    async with owner_scoped_session() as session:
        await delete_data(session)
    await owner_scoped_session.remove()
    yield True
    async with owner_scoped_session() as session:
        await delete_data(session)
    await owner_scoped_session.remove()


@pytest.fixture(scope="module")
def app(init_database) -> FastAPI:
    from hyperknowledge.eventdb.server import app

    return app


@pytest.fixture(scope="module")
async def client(app) -> Iterator[AsyncClient]:
    async with LifespanManager(app):
        async with AsyncClient(
            app=app, base_url="http://test", transport=ASGIWebSocketTransport(app)
        ) as ac:
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


@pytest.fixture(scope="function")
async def admin_agent(clean_tables, client):
    from hyperknowledge.eventdb.schemas import AgentModelWithPw

    admin_agent = AgentModelWithPw(
        username="admin", email="admin@example.com", passwd="admin", is_admin=True
    )
    response = await client.post("/agents", json=admin_agent.model_dump())
    assert response.status_code == 201, response.json()
    return admin_agent


@pytest.fixture(scope="function")
async def admin_token(admin_agent, client) -> str:
    form = OAuth2PasswordRequestForm(username="admin", password="admin", grant_type='password')
    response = await client.post("/token", data=form.__dict__)
    assert response.status_code == 200, response.json()
    return response.json()["access_token"]


@pytest.fixture(scope="function")
async def loaded_context(admin_token, client, hk_events_ctx) -> Dict:
    headers = dict(Authorization=f"Bearer {admin_token}")
    response = await client.post(
        "/context",
        json=dict(
            url="https://hyperknowledge.org/schemas/hyperknowledge_events.jsonld",
            ctx=hk_events_ctx,
        ),
        headers=headers,
    )
    assert response.status_code == 201
    return hk_events_ctx
    # TODO: Delete context?


@pytest.fixture(scope="function")
async def loaded_schema(admin_token, simple_schema, loaded_context, client) -> Dict:
    headers = dict(Authorization=f"Bearer {admin_token}")
    response = await client.post("/schema", json=simple_schema, headers=headers)
    assert response.status_code == 201, response.json()
    yield simple_schema
    await client.delete("/schema/ex", headers=headers)


@pytest.fixture(scope="function")
async def loaded_handlers(admin_token, loaded_schema, handlers, client) -> Dict:
    headers = dict(Authorization=f"Bearer {admin_token}")
    response = await client.post("/handler", json=handlers, headers=headers)
    assert response.status_code == 201, response.json()
    return handlers
    # TODO: Allow deleting handlers


@pytest.fixture(scope="function")
async def quidam_agent(admin_token, client):
    from hyperknowledge.eventdb.schemas import AgentModelWithPw

    headers = dict(Authorization=f"Bearer {admin_token}")
    agent = AgentModelWithPw(
        username="quidam", email="quidam@example.com", passwd="quidam"
    )
    response = await client.post("/agents", json=agent.model_dump())
    assert response.status_code == 201, response.json()

    # Confirm the non-admin user by the admin user, and give them add_source
    response = await client.patch(
        "/agents/quidam",
        json=dict(confirmed=True, permissions=["add_source"]),
        headers=headers,
    )
    assert response.status_code == 200, response.json()
    return agent


@pytest.fixture(scope="function")
async def quidam_token(quidam_agent, client) -> str:
    form = OAuth2PasswordRequestForm(username="quidam", password="quidam", grant_type='password')
    response = await client.post("/token", data=form.__dict__)
    assert response.status_code == 200, response.json()
    return response.json()["access_token"]


@pytest.fixture(scope="function")
async def quidam_test_source(quidam_token, client):
    from hyperknowledge.eventdb.schemas import LocalSourceModel

    headers = dict(Authorization=f"Bearer {quidam_token}")
    source = LocalSourceModel(local_name="test")
    response = await client.post("/source", json=source.model_dump(), headers=headers)
    assert response.status_code == 201, response.json()
    yield LocalSourceModel.model_validate(response.json())
    await client.delete("/source/test", headers=headers)


@pytest.fixture(scope="function")
async def quidam_dep_test_source(quidam_token, quidam_test_source, client):
    from hyperknowledge.eventdb.schemas import LocalSourceModel

    assert quidam_test_source.id
    headers = dict(Authorization=f"Bearer {quidam_token}")
    source = LocalSourceModel(
        local_name="test2", included_source_ids=[quidam_test_source.id]
    )
    response = await client.post("/source", json=source.model_dump(), headers=headers)
    assert response.status_code == 201, response.json()
    yield LocalSourceModel.model_validate(response.json())
    await client.delete("/source/test", headers=headers)
