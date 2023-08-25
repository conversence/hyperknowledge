"""The SQLAlchemy models for the base machinery"""

from __future__ import annotations
from typing import List, Union, Optional
import re
from uuid import UUID as UUIDv

from rdflib import URIRef
from sqlalchemy import ForeignKey, String, Text, Boolean, Integer, Table, Column, select, delete, update
from sqlalchemy.sql.functions import func, GenericFunction
from sqlalchemy.sql.expression import join, column
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr, foreign, remote, joinedload, aliased, column_property
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, ENUM, UUID, TIMESTAMP, BYTEA
from sqlalchemy_utils import LtreeType

from . import dbTopicId, PydanticURIRef

class classproperty(object):
    def __init__(self, f):
        self.f = f
    def __get__(self, obj, owner):
        return self.f(owner)


class Base(DeclarativeBase):
    pass


id_type = ENUM(
    # identifiers
    'vocabulary',
    'term',
    'uuid',
    'concepts',
    # special cases
    'source',  # the reference is an url
    # literals and value objects
    'langstring',
    'binary_data',
    'struct',
    name='id_type')


struct_type = ENUM(
    'hk_schema',
    'ontology',
    'ld_context',
    'other',
    name='struct_type')


event_handler_language = ENUM(
    'javascript',
    'wasm',
    'python',
    name='event_handler_language'
)

permission = ENUM(
    'add_schema',
    'add_source',
    'add_handler',
    'admin',
    name='permission'
)


class Topic(Base):
    """Anything that can be talked about or referred to is a topic. The root class.
    """
    __tablename__ = 'topic'
    __mapper_args__ = {
        "polymorphic_abstract": True,
        "polymorphic_on": "base_type",
        "with_polymorphic": "*",
    }
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, primary_key=True)
    base_type: Mapped[id_type] = mapped_column(id_type, nullable=False)
    has_projections: Mapped[List[dbTopicId]] = mapped_column(ARRAY(dbTopicId), nullable=False, server_default='{}')

    @classmethod
    async def get_by_uri(cls, session, value) -> Optional[Topic]:
        if not isinstance(value, str):
            return
        if value.startswith('urn:uuid:'):
            r = await session.execute(select(UUIDentifier).filter_by(value=UUIDv(value[9:])))
            r = r.one_or_none()
            if r:
                return r[0]
            return
        q = select(Vocabulary)
        if voc_split := re.match(r'(.*)\b(\w+)$', value):
            voc, term = voc_split.groups()
            q = q.filter((Vocabulary.uri == value) | (Vocabulary.uri == voc))
        else:
            q = q.filter(Vocabulary.uri == value)
        r = await session.execute(q)
        r = r.one_or_none()
        if r:
            voc = r[0]
            if voc.uri == value:
                return voc
            r = await session.execute(select(Term).filter_by(term=term, vocabulary=voc))
            r = r.one_or_none()
        if r:
            return r[0]

    @classmethod
    async def ensure_url(cls, session, value: PydanticURIRef, ensure_term: bool=False) -> Topic:
        if value.startswith('urn:uuid:'):
            return await UUIDentifier.ensure(session, value[9:])
        q = select(Vocabulary)
        if voc_split := re.match(r'(.*)\b(\w+)$', value):
            voc, term = voc_split.groups()
            q = q.filter((Vocabulary.uri == value) | (Vocabulary.uri == voc))
        else:
            q = q.filter(Vocabulary.uri == value)
        r = await session.execute(select(Vocabulary).filter_by(uri=value))
        if r := r.one_or_none():
            voc = r[0]
            if voc.uri == value:
                return voc
            return await Term.ensure(session, term, voc.id)
        if ensure_term:
            assert voc_split
            voc = await Vocabulary.ensure(session, voc)
            await session.flush()
            return await Term.ensure(session, term, voc.id)
        # should I create as a voc or as a term? Assume voc-less term for now
        # Note: I should be able to set voc a posteriori on voc-less terms
        return await Vocabulary.ensure(session, value)


# Those are almost certainly Identifiers
Topic.projections: Mapped[List[Topic]] = relationship(Topic, primaryjoin=foreign(Topic.has_projections).contains(remote(Topic.id)), viewonly=True)


class ensure_vocabulary(GenericFunction):
    type = dbTopicId
    inherit_cache = True


class Vocabulary(Topic):
    __tablename__ = 'vocabulary'
    __mapper_args__ = {
        "polymorphic_identity": "vocabulary",
    }
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Topic.id), primary_key=True)
    uri: Mapped[String] = mapped_column(String, nullable=False, unique=True)
    prefix: Mapped[String] = mapped_column(String, unique=True)

    @classmethod
    async def ensure(cls, session, vocabulary, prefix=None) -> Vocabulary:
        id_ = await session.scalar(ensure_vocabulary(vocabulary, prefix))
        await session.flush()
        return await session.get(cls, id_)


schema_defines_table = Table(
    'schema_defines', Base.metadata,
    Column('term_id', dbTopicId, ForeignKey('term.id'), primary_key=True),
    Column('schema_id', dbTopicId, ForeignKey('struct.id'), primary_key=True)
)


class ensure_term(GenericFunction):
    type = dbTopicId
    inherit_cache = True

class ensure_term_with_voc(GenericFunction):
    type = dbTopicId
    inherit_cache = True


class Term(Topic):
    __tablename__ = 'term'
    __mapper_args__ = {
        "polymorphic_identity": "term",
    }
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Topic.id), primary_key=True)
    term: Mapped[String] = mapped_column(String, nullable=False)
    vocabulary_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Vocabulary.id))
    vocabulary = relationship(Vocabulary, primaryjoin=Vocabulary.id==vocabulary_id, lazy='joined')
    schema: Mapped[Struct] = relationship("Struct", secondary=schema_defines_table, back_populates='terms')

    @property
    def uri(self) -> PydanticURIRef:
        return PydanticURIRef(self.vocabulary.uri+self.term)

    @classmethod
    async def ensure(cls, session, term: str, vocabulary: Union[None, str, int]=None, prefix: str=None) -> Term:
        if prefix or isinstance(vocabulary, str):
            vocabulary = await session.scalar(ensure_term(term, vocabulary, prefix))
        else:
            vocabulary = await session.scalar(ensure_term_with_voc(term, vocabulary))
        await session.flush()
        return await session.get(cls, vocabulary)


class ensure_uuid(GenericFunction):
    type = dbTopicId
    inherit_cache = True


class UUIDentifier(Topic):
    __tablename__ = 'uuidentifier'
    __mapper_args__ = {
        "polymorphic_identity": "uuid",
    }
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Topic.id), primary_key=True)
    value: Mapped[UUID] = mapped_column(UUID, nullable=False)

    @property
    def uri(self) -> PydanticURIRef:
        return PydanticURIRef(f'urn:uuid:{self.value}')

    @classmethod
    async def ensure(cls, session, uuid=None) -> UUIDentifier:
        id_ = await session.scalar(ensure_uuid(uuid))
        await session.flush()
        return  await session.get(cls, id_)


class ensure_langstring(GenericFunction):
    type = dbTopicId
    inherit_cache = True


class LangString(Topic):
    __tablename__ = 'langstring'
    __mapper_args__ = {
        "polymorphic_identity": "langstring",
    }
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Topic.id), primary_key=True)
    value: Mapped[String] = mapped_column(String, nullable=False)
    lang: Mapped[LtreeType] = mapped_column(LtreeType)

    @classmethod
    async def ensure(cls, session, value: str, langtag: str) -> LangString:
        id_ = await session.scalar(ensure_langstring(value, langtag))
        await session.flush()
        return  await session.get(cls, id_)


class ensure_struct(GenericFunction):
    type = dbTopicId
    inherit_cache = True


class Struct(Topic):
    __tablename__ = 'struct'
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Topic.id), primary_key=True)
    value: Mapped[JSONB] = mapped_column(JSONB, nullable=False)
    entities: Mapped[List[dbTopicId]] = mapped_column(ARRAY(dbTopicId))
    hash: Mapped[BYTEA] = mapped_column(BYTEA, server_default='sha256(value::varchar::bytea)')
    subtype: Mapped[struct_type] = mapped_column(struct_type, server_default='other')
    data_schema_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Topic.id))
    is_vocab: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Vocabulary.id))

    __mapper_args__ = dict(
        polymorphic_identity = "struct",
        inherit_condition = id == Topic.id
    )

    data_schema: Mapped[Topic] = relationship(Topic, primaryjoin=remote(Topic.id) == foreign(data_schema_id))
    as_voc: Mapped[Vocabulary] = relationship(Vocabulary, primaryjoin=remote(Topic.id) == foreign(is_vocab))
    terms: Mapped[List[Term]] = relationship(Term, secondary=schema_defines_table, back_populates='schema')

    @classmethod
    async def ensure(cls, session, value: JSONB, subtype: struct_type='other', url: str=None, prefix: str=None, schema_type: str=None) -> Struct:
        id_ = await session.scalar(ensure_struct(func.cast(value, JSONB), func.cast(subtype, struct_type), url, prefix, schema_type))
        await session.flush()
        return  await session.get(cls, id_, options=(joinedload(cls.data_schema), joinedload(cls.as_voc)))



class ensure_binary_data(GenericFunction):
    type = dbTopicId
    inherit_cache = True


class BinaryData(Topic):
    __tablename__ = 'binary_data'
    __mapper_args__ = {
        "polymorphic_identity": "binary_data"
    }

    id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Topic.id), primary_key=True)
    value: Mapped[BYTEA] = mapped_column(BYTEA, nullable=False)
    hash: Mapped[BYTEA] = mapped_column(BYTEA, server_default='sha256(value::varchar::bytea)')

    @classmethod
    async def ensure(cls, session, value: str) -> Struct:
        id_ = await session.scalar(ensure_binary_data(value))
        await session.flush()
        return  await session.get(cls, id_)


class ensure_source(GenericFunction):
    type = dbTopicId
    inherit_cache = True


class Agent(Base):
    __tablename__ = 'agent'
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, primary_key=True)
    email: Mapped[String] = mapped_column(String, nullable=False, unique=True)
    username: Mapped[String] = mapped_column(String, nullable=False, unique=True)
    passwd: Mapped[String] = mapped_column(String, nullable=False)
    confirmed: Mapped[Boolean] = mapped_column(Boolean, server_default='false')
    permissions: Mapped[List[permission]] = mapped_column(ARRAY(permission), server_default='{}')
    created: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP, server_default="now() AT TIME ZONE 'UTC'")
    last_login: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP)
    last_login_email_sent: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP)

    source_permissions: Mapped[List[AgentSourcePermission]] = relationship("AgentSourcePermission", back_populates="agent")
    source_selective_permissions: Mapped[List[AgentSourceSelectivePermission]] = relationship("AgentSourceSelectivePermission", back_populates="agent")
    processors: Mapped[List[EventProcessor]] = relationship("EventProcessor", back_populates="owner")

    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions or 'admin' in self.permissions

source_inclusion_table = Table(
    'source_inclusion', Base.metadata,
    Column('included_id', dbTopicId, ForeignKey('source.id'), primary_key=True),
    Column('including_id', dbTopicId, ForeignKey('source.id'), primary_key=True)
)

class including_sources(GenericFunction):
    type: List[dbTopicId]

class Source(Vocabulary):
    __tablename__ = 'source'
    __mapper_args__ = {
        "polymorphic_identity": "source",
    }
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Vocabulary.id), primary_key=True)
    creator_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Agent.id))
    local_name: Mapped[String] = mapped_column(String, unique=True)
    public_read: Mapped[Boolean] = mapped_column(Boolean, server_default='true')
    public_write: Mapped[Boolean] = mapped_column(Boolean, server_default='false')
    selective_write: Mapped[Boolean] = mapped_column(Boolean, server_default='false')

    last_event_t: Mapped[LastEvent] = relationship(back_populates='source')
    creator: Mapped[Agent] = relationship(Agent, primaryjoin=creator_id==Agent.id)

    included_source_ids: Mapped[List[dbTopicId]] = column_property(select(func.array_agg(source_inclusion_table.c.included_id)).filter(source_inclusion_table.c.including_id==id).scalar_subquery())
    included_sources: Mapped[List[Source]] = relationship("Source", secondary=source_inclusion_table, primaryjoin=id==source_inclusion_table.c.including_id, secondaryjoin=source_inclusion_table.c.included_id==id, back_populates="including_sources")
    including_sources: Mapped[List[Source]] = relationship("Source", secondary=source_inclusion_table, primaryjoin=id==source_inclusion_table.c.included_id, secondaryjoin=source_inclusion_table.c.including_id==id, back_populates="included_sources")

    included_source_ids_rec: Mapped[List[dbTopicId]] = column_property(func.included_sources(foreign(id), type_=ARRAY(dbTopicId)), deferred=True)
    including_source_ids_rec: Mapped[List[dbTopicId]] = column_property(func.including_sources(foreign(id), type_=ARRAY(dbTopicId)), deferred=True)

    included_sources_rec: Mapped[List[Source]] = relationship("Source", viewonly=True, uselist=True,
        primaryjoin="Source.included_source_ids_rec.any(remote(Source.id))")
    including_sources_rec: Mapped[List[Source]] = relationship("Source", viewonly=True, uselist=True,
        primaryjoin="Source.including_source_ids_rec.any(remote(Source.id))")

    def filter_event_query(self, query, alias=None):
        target = alias or Event
        return query.filter(Source.included_source_ids_rec.any(target.source_id), Source.id==self.id)

    @classmethod
    async def ensure(cls, session, uri: str, local_name: str, public_read: bool=True, public_write: bool=False, selective_write: bool=False, includes_source_ids: List[int]=[]) -> Source:
        id_ = await session.scalar(ensure_source(uri, local_name, public_read, public_write, selective_write, includes_source_ids))
        await session.flush()
        return  await session.get(cls, id_)


class EventHandler(Base):
    __tablename__ = 'event_handler'
    id: Mapped[Integer] = mapped_column(Integer, primary_key=True)
    event_type_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Term.id), nullable=False)
    target_role: Mapped[String] = mapped_column(String, nullable=False)
    target_range_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Term.id), nullable=False)
    language: Mapped[event_handler_language] = mapped_column(event_handler_language, nullable=False)
    code_text: Mapped[Text] = mapped_column(Text)
    code_binary: Mapped[BYTEA] = mapped_column(BYTEA)

    event_type: Mapped[Term] = relationship(Term, primaryjoin=event_type_id==Term.id)
    target_range: Mapped[Term] = relationship(Term, primaryjoin=target_range_id==Term.id)


class Event(Base):
    __tablename__ = 'event'
    source_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Source.id), primary_key=True)
    creator_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Agent.id))
    event_type_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Term.id), nullable=False)
    created: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP, primary_key=True, server_default='now()')
    data: Mapped[JSONB] = mapped_column(JSONB, nullable=False)
    active: Mapped[Boolean] = mapped_column(Boolean, nullable=False, server_default='true')

    source: Mapped[Source] = relationship(Source, primaryjoin=source_id == Source.id)
    creator: Mapped[Agent] = relationship(Agent, primaryjoin=creator_id == Agent.id)
    event_type: Mapped[Term] = relationship(Term, primaryjoin=event_type_id==Term.id)
    handlers: Mapped[List[EventHandler]] = relationship(
        EventHandler, primaryjoin=foreign(event_type_id)==remote(EventHandler.event_type_id), viewonly=True, uselist=True)

    including_source_ids_rec = column_property(func.including_sources(foreign(source_id), type_=ARRAY(dbTopicId)), deferred=True)

    included_in_sources: Mapped[List[Source]] = relationship(Source, viewonly=True, uselist=True,
        primaryjoin="Event.including_source_ids_rec.any(remote(Source.id))")


class LastEvent(Base):
    __tablename__ = 'last_event'
    source_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Source.id), primary_key=True)
    last_event_ts: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP)

    source: Mapped[Source] = relationship(back_populates="last_event_t")


class EventProcessor(Base):
    __tablename__ = 'event_processor'
    id: Mapped[Integer] = mapped_column(Integer, primary_key=True)
    name: Mapped[String] = mapped_column(String, nullable=False)
    owner_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Agent.id), nullable=False)
    source_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Source.id), nullable=True)
    last_event_ts: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP)

    owner: Mapped[Agent] = relationship(Agent, back_populates="processors")
    source: Mapped[Source] = relationship(Source)


class ProjectionTable(Base):
    __tablename__ = 'projection_table'
    schema_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Struct.id), primary_key=True)
    tablename: Mapped[String] = mapped_column(String, primary_key=True)



class AgentSourcePermission(Base):
    __tablename__ = 'agent_source_permission'
    agent_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Agent.id), primary_key=True)
    source_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Source.id), primary_key=True)
    is_admin: Mapped[Boolean] = mapped_column(Boolean, server_default='false')
    allow_read: Mapped[Boolean] = mapped_column(Boolean, server_default='false')
    allow_write: Mapped[Boolean] = mapped_column(Boolean, server_default='false')
    allow_all_write: Mapped[Boolean] = mapped_column(Boolean, server_default='false')
    is_request: Mapped[Boolean] = mapped_column(Boolean, server_default='false')

    agent: Mapped[Agent] = relationship(Agent, back_populates="source_permissions")
    source: Mapped[Source] = relationship(Source)


class AgentSourceSelectivePermission(Base):
    __tablename__ = 'agent_source_selective_permission'
    agent_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Agent.id), primary_key=True)
    source_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Source.id), primary_key=True)
    event_type_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Term.id), primary_key=True)
    is_request: Mapped[Boolean] = mapped_column(Boolean, server_default='false')

    agent: Mapped[Agent] = relationship(Agent, back_populates="source_selective_permissions")
    source: Mapped[Source] = relationship(Source)
    event_type: Mapped[Term] = relationship(Term)


class ProjectionMixin:
    id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Topic.id), primary_key=True)
    source_id: Mapped[dbTopicId] = mapped_column(dbTopicId, ForeignKey(Source.id), primary_key=True)
    event_time: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP, primary_key=True)
    obsolete: Mapped[TIMESTAMP] = mapped_column(TIMESTAMP, nullable=True)

    @declared_attr
    def source(cls) -> Mapped[Source]:
        return relationship(Source, primaryjoin=Source.id==cls.source_id)

    @declared_attr
    def last_event(cls) -> Mapped[Event]:
        return relationship(Event, primaryjoin=(
            remote(Event.source_id)==foreign(cls.source_id))
         & (remote(Event.created)==foreign(cls.event_time)))

    @declared_attr
    def identifier(cls) -> Mapped[Topic]:
        return relationship(Topic, primaryjoin=Topic.id==cls.id)


async def delete_data(session):
    # Delete structures separately to delete generated tables and their dependencies
    await session.execute(delete(Topic).filter(Topic.id.in_(select(Struct.id).filter_by(subtype='hk_schema'))))
    await session.execute(delete(Topic))
    await session.execute(delete(Agent).filter(Agent.id > 0))
    await session.execute(delete(EventProcessor).filter(EventProcessor.owner_id > 0))
    await session.execute(update(EventProcessor).values(last_event_ts = None))
    await session.commit()
