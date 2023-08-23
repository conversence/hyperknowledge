import pytest
from asyncio import sleep

from fastapi.security import OAuth2PasswordRequestForm

pytestmark = pytest.mark.anyio


async def test_no_schema(client):
    response = await client.get("/schema")
    assert response.status_code == 200
    assert response.json() == []


async def test_add_schema(client, loaded_context, admin_token, simple_schema):
    headers = dict(Authorization=f"Bearer {admin_token}")
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


async def test_add_event(client, loaded_handlers, quidam_token, quidam_test_source):
    from hyperknowledge.eventdb.schemas import GenericEventModel
    headers = dict(Authorization=f"Bearer {quidam_token}")
    # Post the event
    event = GenericEventModel(data={
        "@type": "ex:create_document",
        "title": {"@value": "A title", "@lang": "en"},
        "url": "http://example.com/doc0"})
    response = await client.post(f"/source/{quidam_test_source.local_name}/events", json=event.model_dump(), headers=headers)
    assert response.status_code == 201, response.json()
    result = response.json()
    doc_id = result['data']['target']
    assert doc_id.startswith("urn:uuid:")
    # Let the event loop catch up
    await sleep(0.5)
    # Look at the projection
    response = await client.get(f"/source/{quidam_test_source.local_name}/topic/{doc_id[9:]}/ex:document", headers=headers)
    assert response.status_code == 200, response.json()
    result = response.json()
    assert result["@id"] == doc_id
    # Post an update event
    event = GenericEventModel(data={
        "@type": "ex:update_document",
        "target": doc_id,
        "url": "http://example.com/doc1"})
    response = await client.post(f"/source/{quidam_test_source.local_name}/events", json=event.model_dump(), headers=headers)
    assert response.status_code == 201, response.json()
    result = response.json()
    # Let the event loop catch up
    await sleep(0.5)
    # Look at the projection
    response = await client.get(f"/source/{quidam_test_source.local_name}/topic/{doc_id[9:]}/ex:document", headers=headers)
    assert response.status_code == 200, response.json()
    result = response.json()
    assert result["url"].endswith('1')
    assert result['title'], "Title got clobbered"


async def test_dependent_source(client, loaded_handlers, quidam_token, quidam_test_source, quidam_dep_test_source):
    from hyperknowledge.eventdb.schemas import GenericEventModel
    headers = dict(Authorization=f"Bearer {quidam_token}")
    # Post the event
    event = GenericEventModel(data={
        "@type": "ex:create_document",
        "title": {"@value": "A title", "@lang": "en"},
        "url": "http://example.com/doc0"})
    response = await client.post(f"/source/{quidam_test_source.local_name}/events", json=event.model_dump(), headers=headers)
    assert response.status_code == 201, response.json()
    result = response.json()
    doc_id = result['data']['target']
    assert doc_id.startswith("urn:uuid:")
    # Let the event loop catch up
    await sleep(0.5)
    # Look at the projection on the base
    response = await client.get(f"/source/{quidam_test_source.local_name}/topic/{doc_id[9:]}/ex:document", headers=headers)
    assert response.status_code == 200, response.json()
    result = response.json()
    assert result["@id"] == doc_id
    response = await client.get(f"/source/{quidam_dep_test_source.local_name}/topic/{doc_id[9:]}/ex:document", headers=headers)
    assert response.status_code == 200, response.json()
    result = response.json()
    assert result["@id"] == doc_id
