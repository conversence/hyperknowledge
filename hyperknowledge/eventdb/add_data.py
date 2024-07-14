"""Utility script to add data to the schemas outside of the server process"""

from typing import Union
import argparse
import anyio
from pathlib import Path
from json import load

import grequests
from yaml import safe_load
from sqlalchemy import select

from hyperknowledge.eventdb import owner_scoped_session
from .models import Struct, EventHandler, Term, Vocabulary
from .schemas import HkSchema, EventHandlerSchemas
from .make_tables import process_schema
from .context import Context


async def read_struct(url: str, fname: Union[str, Path, None] = None):
    if fname:
        if isinstance(fname, str):
            fname = Path(fname)
        suffix = fname.suffix.lower().lstrip(".")
        with open(fname) as f:
            if suffix in ("json", "jsonld"):
                return load(f)
            if suffix in ("yml", "yaml"):
                return safe_load(f)
    else:
        r = await grequests.get(url)
        return r.json


async def add_schema(
    url: str, prefix: str, fname: Path = None, overwrite: bool = False
):
    jsonf = await read_struct(url, fname)
    schema = HkSchema.model_validate(jsonf)
    async with owner_scoped_session() as session:
        await Struct.ensure(session, jsonf, "hk_schema", url, prefix)
        # Ensure all the terms
        await process_schema(schema, jsonf, url, prefix, overwrite, session)
        await session.commit()


async def add_handlers(url: str, fname: Path = None, overwrite=False):
    jsonf = await read_struct(url, fname)
    schema = EventHandlerSchemas.model_validate(jsonf)
    async with owner_scoped_session() as session:
        for handler in schema.handlers:
            event_prefix, event_term = schema.context.shrink_iri(
                handler.event_type
            ).split(":")
            event_voc = schema.context.expand(f"{event_prefix}:")
            event_type = await Term.ensure(session, event_term, event_voc, event_prefix)
            range_prefix, range_term = schema.context.shrink_iri(
                handler.target_range
            ).split(":")
            range_voc = schema.context.expand(f"{range_prefix}:")
            target_range = await Term.ensure(
                session, range_term, range_voc, range_prefix
            )
            # TODO: Check that target_range is a valid projection, and that the target_role is part of the event's schema
            existing = await session.execute(
                select(EventHandler).filter_by(
                    event_type=event_type,
                    target_range=target_range,
                    target_role=handler.target_role,
                )
            )
            if existing := existing.first():
                if overwrite:
                    existing = existing[0]
                    existing.language = handler.language
                    existing.code_text = handler.code_text
                    existing.code_binary = None
            else:
                session.add(
                    EventHandler(
                        event_type=event_type,
                        target_range=target_range,
                        target_role=handler.target_role,
                        language=handler.language,
                        code_text=handler.code_text,
                    )
                )
        await session.commit()
    return schema


# TODO: removing handlers


async def add_context_data(url: str, fname: Path = None, overwrite: bool = False):
    data = await read_struct(url, fname)
    # First make sure it's valid
    Context(data, base=url)
    async with owner_scoped_session() as session:
        vocab = await Vocabulary.ensure(session, url)
        context_struct = await session.scalar(
            select(Struct).filter_by(is_vocab=vocab.id, subtype="ld_context")
        )
        if context_struct:
            if not overwrite:
                return context_struct
            context_struct.value = data
        else:
            context_struct = Struct(value=data, subtype="ld_context", is_vocab=vocab.id)
            session.add(context_struct)
        await session.commit()
    return context_struct


async def add_data(
    data_type: str,
    url: str,
    prefix: str = None,
    file: Path = None,
    overwrite: bool = False,
):
    if data_type == "hk_schema":
        return await add_schema(url, prefix, file, overwrite)
    elif data_type == "hk_handlers":
        return await add_handlers(url, file, overwrite)
    elif data_type == "context":
        return await add_context_data(url, file, overwrite)
    elif data_type == "ontology":
        raise NotImplementedError()
    else:
        raise RuntimeError(f"Unknown type: {data_type}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "type",
        choices=["context", "hk_schema", "ontology", "hk_handlers"],
        help="data type",
    )
    parser.add_argument("-u", "--url", help="schema url")
    parser.add_argument("-p", "--prefix", help="schema prefix")
    parser.add_argument("-f", "--file", type=Path, help="schema file (optional)")
    parser.add_argument(
        "-o", "--overwrite", action="store_true", help="overwrite existing tables"
    )
    args = parser.parse_args()
    anyio.run(
        add_data,
        args.type,
        args.url,
        args.prefix,
        args.file,
        args.overwrite,
        backend_options={"debug": True},
    )
