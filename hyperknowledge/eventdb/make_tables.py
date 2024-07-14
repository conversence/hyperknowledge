"""Generate a SQLAlchemy model from the HK schema, and create the corresponding Postgres table."""

from typing import List, Tuple, Dict
from itertools import chain

from sqlalchemy import (
    Column, String, Boolean, Date, Time, Float, Integer, BINARY, Text, select, text)
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.dialects.postgresql import (INTERVAL, TIMESTAMP, ARRAY, insert)
from pydantic import json, BaseModel
from rdflib.namespace import XSD, RDF, RDFS

from . import dbTopicId, PydanticURIRef, as_tuple, as_tuple_or_scalar, owner_scoped_session
from .models import Base, ProjectionMixin, ProjectionTable, Struct, schema_defines_table, Term, Topic
from .schemas import (
    HkSchema, ProjectionAttributeSchema, ProjectionSchema, models_from_schemas,
    getProjectionSchemas, scalar_field_types)
from .auth import escalated_session


column_types = {
    XSD.anyURI: String,
    XSD.base64Binary: BINARY,
    XSD.boolean: Boolean,
    XSD.date: Date,
    XSD.dateTime: TIMESTAMP,
    XSD.decimal: Integer,
    XSD.double: Float,
    XSD.duration: INTERVAL,
    XSD.float: Float,
    XSD.hexBinary: BINARY,
    XSD.gDay: Integer,
    XSD.gMonth: Integer,
    XSD.gMonthDay: Integer,
    XSD.gYear: Integer,
    XSD.gYearMonth: Integer,
    # XSD.NOTATION: None,
    XSD.QName: String,
    XSD.string: Text,
    XSD.time: Time,
    XSD.language: String,

    RDFS.Resource: dbTopicId,
    RDF.langString: Text,
}

# Would be nice to have a jsonb type?


def as_column(schema: ProjectionAttributeSchema) -> List[Column]:
    # Non-scalars are dbTopicId
    column_type = column_types.get(as_tuple_or_scalar(schema.range), dbTopicId)
    if not schema.functional:
        column_type = ARRAY(column_type)
    # TODO: Foreign keys and other check constraints
    # Question: How to distinguish projection types from generic RDF types here?
    if schema.range == RDF.langString:
        return [Column(schema.name, column_type, nullable=schema.optional),
                Column(f"{schema.name}_lang", String, nullable=schema.optional)]
    else:
        # Including union tuples
        return [Column(schema.name, column_type, nullable=schema.optional)]


def make_table(schema: ProjectionSchema, prefix: str, reloading=False) -> type[Base]:
    classname = f'{prefix.title()}{schema.name.title()}'
    columns = list(chain(*[as_column(s) for s in schema.attributes]))
    attributes = {c.name: c for c in columns}
    attributes['__tablename__'] = f'{prefix}__{schema.name}'
    if reloading:
        attributes['__table_args__'] = dict(extend_existing=True)
    return type(classname, (Base, ProjectionMixin), attributes)


KNOWN_DB_MODELS: Dict[PydanticURIRef, type[Base]] = {}


def make_projections(schema: HkSchema, reload=False) -> List[Base]:
    global KNOWN_DB_MODELS
    ctx = schema.context
    base = getattr(ctx, 'vocab', None) or getattr(ctx, '_base', None)
    assert base, "The schema needs to have a _base or vocab"
    prefix = ctx._prefixes[base]
    assert prefix, "The schema base needs to have a prefix"
    # TODO: Ensure prefix uniqueness against the database
    classes = []

    mapped_classes = {m.class_.__name__: m.class_ for m in Base.registry.mappers}
    for s in schema.projectionSchemas.values():
        cls_name = f'{prefix.title()}{s.name.title()}'
        cls = mapped_classes.get(cls_name)
        if reload or not cls:
            cls = make_table(s, prefix, cls and reload)
        classes.append(cls)
        KNOWN_DB_MODELS[s.type] = cls
    return classes


async def read_existing_projections(reload=False) -> Tuple[List[HkSchema], List[Base]]:
    schemas: List[HkSchema] = []
    classes: List[Base] = []
    async with owner_scoped_session() as session:
        r = await session.execute(select(Struct).filter_by(subtype='hk_schema'))
        for (schema_struct, ) in r:
            schema = HkSchema.model_validate(schema_struct.value)
            schemas.append(schema)
            new_classes = make_projections(schema, reload)
            classes.extend(new_classes)
    return schemas, classes



async def create_tables(schema: HkSchema, schema_struct_id: dbTopicId, overwrite: bool = False, session = None) -> List[Base]:
    if not session:
        async with owner_scoped_session() as session:
            r = await create_tables(schema, schema_struct_id, overwrite, session)
            await session.commit()
            return r
    classes = make_projections(schema)
    for cls in classes:
        tablename = cls.__table__.key
        r = await session.execute(select(ProjectionTable).filter_by(schema_id=schema_struct_id, tablename=tablename))
        row = r.first()
        if row and overwrite:
            await session.delete(row[0])
            await session.commit()
        if overwrite or not row:
            async with escalated_session(session) as esession:
                await esession.execute(CreateTable(cls.__table__))
                esession.add(ProjectionTable(schema_id=schema_struct_id, tablename=tablename))
                db_name = str(esession.bind.engine.url).split('/')[-1]
                await esession.execute(text(f"GRANT SELECT ON TABLE public.{tablename} to {db_name}__client"))
                await esession.execute(text(f"GRANT SELECT ON TABLE public.{tablename} to {db_name}__member"))
                await esession.execute(text(f"ALTER TABLE public.{tablename} ENABLE ROW LEVEL SECURITY"))
                await esession.execute(text(f"DROP POLICY IF EXISTS {tablename}_select_policy ON public.{tablename}"))
                await esession.execute(text(f"CREATE POLICY {tablename}_select_policy ON public.{tablename} FOR SELECT USING (can_read_source(source_id))"))
    return classes


# TODO: create indices

async def process_schema(schema: HkSchema, schema_json: json, url: str, prefix: str, overwrite: bool=False, session=None) -> Struct:
    if session is None:
        async with owner_scoped_session() as session:
            s = await process_schema(schema, schema_json, url, prefix, overwrite, session)
            await session.commit()
            return s

    ctx = schema.context
    # Sanity check on context
    base = getattr(ctx, 'vocab', None) or getattr(ctx, '_base', None)
    assert base, "The schema needs to have a base"
    assert base == url
    assert prefix == ctx._prefixes[base]
    # TODO: If no base/prefix, actually enrich the context instead
    db_schema = await Struct.ensure(session, schema_json, 'hk_schema', url, prefix)
    # Ensure all the terms
    for name in chain(schema.eventSchemas, schema.projectionSchemas):
        term = await Term.ensure(session, name, db_schema.is_vocab)
        await session.execute(
            insert(schema_defines_table).values(term_id=term.id, schema_id=db_schema.id
            ).on_conflict_do_nothing('schema_defines_pkey'))
    # Ensure all the ranges
    for subschema in chain(schema.eventSchemas.values(), schema.projectionSchemas.values()):
        for attrib_schema in subschema.attributes:
            if as_tuple_or_scalar(attrib_schema.range) in scalar_field_types:
                continue
            for range in as_tuple(attrib_schema.range):
                range_prefix, range_term = schema.context.shrink_iri(range).split(':')
                range_vocab = schema.context.expand(f'{range_prefix}:')
                await Term.ensure(session, range_term, range_vocab, range_prefix)
    await create_tables(schema, db_schema.id, overwrite, session)
    make_projections(schema, True)
    models_from_schemas([schema])
    return db_schema



async def projection_to_db(session, projection: BaseModel, schema: ProjectionSchema) -> Dict:
    db_attribs = {}
    for attribS in schema.attributes:
        value = getattr(projection, attribS.name)
        result = value
        # TODO: handle arrays
        if attribS.range == RDF.langString:
            result = value.value if value else None
            db_attribs[f"{attribS.name}_lang"] = value.language if value else None
        elif as_tuple_or_scalar(attribS.range) not in scalar_field_types:
            # Shouldn't I also ensure range here?
            topic = await Topic.ensure_url(session, value)
            if as_tuple_or_scalar(attribS.range) in getProjectionSchemas():
                # Implicitly: Not a tuple.
                range_topic = await Topic.get_by_uri(session, attribS.range)
                # Here we are inferring type. Not sure that's right?
                if range_topic.id not in topic.has_projections:
                    topic.has_projections.append(range_topic.id)
                    flag_modified(topic, 'has_projections')
            result = topic.id
        db_attribs[attribS.name] = result
    return db_attribs


async def db_to_projection(session, db_object: Base, projectionModel: type[BaseModel], schema: ProjectionSchema) -> BaseModel:
    proj_attribs = {}
    for attribS in schema.attributes:
        value = getattr(db_object, attribS.name)
        result = value
        if value and attribS.range == RDF.langString:
            result = {"@value":value, "@lang": getattr(db_object, f'{attribS.name}_lang')}
        elif value and as_tuple_or_scalar(attribS.range) not in scalar_field_types:
            # Including union tuple
            topic = await session.get(Topic, result)
            result = topic.uri
        proj_attribs[attribS.name] = result
        # proj_attribs["@type"] = schema.type  # we'd need to shrink
        proj_attribs["@type"] = projectionModel.model_fields['type'].annotation.__args__[0]
        as_term = await session.get(Topic, db_object.id)
        proj_attribs["@id"] = as_term.uri
    return projectionModel.model_validate(proj_attribs)
