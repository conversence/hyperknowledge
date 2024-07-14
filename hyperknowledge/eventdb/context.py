"""This defines a subclass of rdflib's JsonLD context which caches the contexts in the database

A lot of trickery to mix sync and async code.
"""

from typing import Optional, Set, Dict
from threading import Thread

import anyio
from sqlalchemy import select
from rdflib import URIRef
from rdflib.plugins.shared.jsonld.errors import (
    INVALID_REMOTE_CONTEXT,
    RECURSIVE_CONTEXT_INCLUSION,
)
from rdflib.plugins.shared.jsonld.keys import CONTEXT
from rdflib.plugins.shared.jsonld.context import Context as rdfContext
from rdflib.plugins.shared.jsonld.util import source_to_json, urljoin

from . import owner_scoped_session
from .models import Struct, Vocabulary


class Context(rdfContext):
    def _fetch_context(
        self, source: str, base: Optional[str], referenced_contexts: Set[str]
    ):
        # As in superclass...
        source_url = urljoin(base, source)  # type: ignore[type-var]

        if source_url in referenced_contexts:
            raise RECURSIVE_CONTEXT_INCLUSION

        referenced_contexts.add(source_url)  # type: ignore[arg-type]

        if source_url in self._context_cache:
            return self._context_cache[source_url]

        # ...except this line
        source = get_context_data(source_url)  # type: ignore[assignment]
        if source and CONTEXT not in source:
            raise INVALID_REMOTE_CONTEXT

        self._context_cache[source_url] = source  # type: ignore[index]

        return source


CONTEXT_CACHE: Dict[URIRef, Context] = {}


async def fetch_context(url: URIRef):
    # This is being called from a thread, so we will create a new collection and scoped context
    # OR... should we use run_coroutine_threadsafe()? Hmmm... probably.
    async with owner_scoped_session() as session:
        engine = session.bind
        vocab = await Vocabulary.ensure(session, url)
        r = await session.execute(
            select(Struct).filter_by(is_vocab=vocab.id, subtype="ld_context")
        )
        if context_struct := r.first():
            data = context_struct[0].value
        else:
            # TODO: wrap in async? in thread?
            data = source_to_json(url)
            session.add(Struct(value=data, subtype="ld_context", is_vocab=vocab.id))
            await session.commit()
    await owner_scoped_session.remove()
    await engine.dispose()
    return data


def get_context_data(url: URIRef):
    # This is a convoluted way to call an async function in a sync context
    result = None

    def work():
        nonlocal result
        result = anyio.run(fetch_context, url)

    t = Thread(target=work)
    t.start()
    t.join()
    return result
