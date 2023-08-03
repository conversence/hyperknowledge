HyperKnowledge Event Sourcing
=============================

Disclaimer: This is currently toy experimental code, not to be used in anything resembling production. Expect radical changes.
It is intended as a basis on which to iterate on the more conceptual HyperKnowledge work.

Overview
--------

The main goal is to define a basic federated event sourcing protocol.
The event and projection schemas are defined using a (currently primitive) schema language that happens to be expressed in JSON-LD.
The Event Handler (the folding function that updates the projection state according to the new event) is defined in javascript. (eventually: also allow wasm.)
A hyperknowledge event node can receive the event schemas and handlers and becomes able to maintain the projections accordingly.
Currently, this is done using a python server; it is expected for this to migrate towards both the database in the client,
allowing for isomorphic and optimistic projection state updates.
Events and projections can be viewed as json-ld objects.
(Eventually, ActivityPub functionality would be nice.)
HyperKnowledge nodes may maintain multiple event streams (aka sources), and accordingly multiple (per-source) versions of projections for a given entity.
There is an explicit disconnect between the identity of entities (aka topics) and that of projections; many projections may exist for any one topic. This corresponds to multiple classes in the OWL world.
It also means anyone can define a new projection on existing events, and/or new events on existing projections, as long as the handlers are defined. The schema definitions are necessarily global. (We need to define versioning.) The handlers are also global, but we could consider per-source handlers.
We want to allow event subscriptions, composite event streams, event federation, etc.
Security is currently non-existent.

Technology stack
----------------

This proof of concept defines a python server over Postgres using SQLAlchemy. The hyperknowledge schemas are interpreted in Pydantic, used to generate both Pydantic models and SQLAlchemy tables. The tables are generated on the fly. (No update as of yet.) This is all exposed using FastAPI.
