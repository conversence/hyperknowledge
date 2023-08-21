import pytest
from asyncio import sleep

from fastapi.security import OAuth2PasswordRequestForm

pytestmark = pytest.mark.anyio


async def test_no_schema(client):
    response = await client.get("/schema")
    assert response.status_code == 200
    assert response.json() == []

async def test_add_schema(client, clean_tables, simple_schema, hk_events_ctx):
    from hyperknowledge.eventdb.schemas import AgentModelWithPw
    # Schemas should be empty to start with
    response = await client.get("/schema")
    assert response.status_code == 200
    assert response.json() == []

    # Create the admin user
    admin_agent = AgentModelWithPw(username='admin', email='admin@example.com', passwd='admin', is_admin=True)
    response = await client.post("/agents", json=admin_agent.model_dump())
    assert response.status_code == 201, response.json()

    # Get the admin token
    form = OAuth2PasswordRequestForm(username="admin", password="admin")
    response = await client.post("/token", data=form.__dict__)
    assert response.status_code == 200, response.json()
    admin_token = response.json()['access_token']
    headers = dict(Authorization=f"Bearer {admin_token}")

    # Optional: Add the context to the cache
    response = await client.post(
        '/context',
        json=dict(url='https://hyperknowledge.org/schemas/hyperknowledge_events.jsonld', ctx=hk_events_ctx),
        headers=headers)
    assert response.status_code == 201

    # Add the schemas
    response = await client.post('/schema', json=simple_schema, headers=headers)
    assert response.status_code == 201
    # Look at resulting schema
    location = response.headers['location']
    response = await client.get(location)
    assert response.status_code == 200
    schema_data = response.json()
    assert schema_data
    assert schema_data['eventSchemas']['create_document']
    assert schema_data['projectionSchemas']['document']

async def test_add_event(client, clean_tables, simple_schema, handlers):
    from hyperknowledge.eventdb.schemas import AgentModelWithPw, LocalSourceModel, GenericEventModel

    # Create the admin user
    admin_agent = AgentModelWithPw(username='admin', email='admin@example.com', passwd='admin', is_admin=True)
    response = await client.post("/agents", json=admin_agent.model_dump())
    assert response.status_code == 201, response.json()

    # Get the admin token
    form = OAuth2PasswordRequestForm(username="admin", password="admin")
    response = await client.post("/token", data=form.__dict__)
    assert response.status_code == 200, response.json()
    admin_token = response.json()['access_token']
    headers = dict(Authorization=f"Bearer {admin_token}")

    # Add the base schema and handlers
    response = await client.post('/schema', json=simple_schema, headers=headers)
    assert response.status_code == 201, response.json()
    response = await client.post('/handler', json=handlers, headers=headers)
    assert response.status_code == 201, response.json()

    # Create a non-admin user
    agent = AgentModelWithPw(username='quidam', email='quidam@example.com', passwd='quidam')
    response = await client.post("/agents", json=agent.model_dump())
    assert response.status_code == 201, response.json()

    # Confirm the non-admin user by the admin user, and give them add_source
    response = await client.patch("/agents/quidam", json=dict(confirmed=True, permissions=['add_source']), headers=headers)
    assert response.status_code == 200, response.json()
    print(response.json())

    # Get the non-admin token
    form = OAuth2PasswordRequestForm(username="quidam", password="quidam")
    response = await client.post("/token", data=form.__dict__)
    assert response.status_code == 200, response.json()
    token = response.json()['access_token']
    headers = dict(Authorization=f"Bearer {token}")

    # Create as source
    source = LocalSourceModel(local_name="test")
    response = await client.post("/source", json=source.model_dump(), headers=headers)
    assert response.status_code == 201, response.json()
    # Post the event
    event = GenericEventModel(data={
        "@type": "ex:create_document",
        "title": {"@value": "A title", "@lang": "en"},
        "url": "http://example.com/doc0"})
    response = await client.post(f"/source/{source.local_name}/events", json=event.model_dump(), headers=headers)
    assert response.status_code == 201, response.json()
    result = response.json()
    doc_id = result['data']['target']
    assert doc_id.startswith("urn:uuid:")
    # Let the event loop catch up
    await sleep(0.5)
    # Look at the projection
    response = await client.get(f"/source/{source.local_name}/topic/{doc_id[9:]}/ex:document", headers=headers)
    assert response.status_code == 200, response.json()
    result = response.json()
    assert result["@id"] == doc_id
    # Post an update event
    event = GenericEventModel(data={
        "@type": "ex:update_document",
        "target": doc_id,
        "url": "http://example.com/doc1"})
    response = await client.post(f"/source/{source.local_name}/events", json=event.model_dump(), headers=headers)
    assert response.status_code == 201, response.json()
    result = response.json()
    # Let the event loop catch up
    await sleep(0.5)
    # Look at the projection
    response = await client.get(f"/source/{source.local_name}/topic/{doc_id[9:]}/ex:document", headers=headers)
    assert response.status_code == 200, response.json()
    result = response.json()
    assert result["url"].endswith('1')
    assert result['title'], "Title got clobbered"
