"""The pydantic schemas for HyperKnowledge event sourcing"""

from __future__ import annotations

from copy import deepcopy
from typing import Optional, Any, List, Dict, TypeVar, Generic, Literal, Tuple, Union, Type
from datetime import datetime, date, timedelta, time
from base64 import urlsafe_b64decode
from functools import reduce
from uuid import UUID
from enum import Enum

from rdflib import URIRef
from rdflib.namespace import XSD, RDF, RDFS
from sqlalchemy.orm.decl_api import DeclarativeBase
from pydantic import BaseModel, Field, field_validator, create_model, ConfigDict, field_serializer, model_validator
from pydantic.fields import FieldInfo

from . import SchemaId, Name, SchemaName, PydanticURIRef, QName, as_tuple_or_scalar
from .context import Context
# from rdflib.plugins.shared.jsonld.context import Context

BaseModelT = TypeVar('BaseModelT', bound=BaseModel)

def make_field_optional(field: FieldInfo, default: Any = None) -> Tuple[Any, FieldInfo]:
    new = deepcopy(field)
    new.default = default
    new.annotation = Optional[field.annotation]  # type: ignore
    return (new.annotation, new)


def to_optional(model: Type[BaseModelT]) -> Type[BaseModelT]:
    """Transform a schema into an equivalent optional schema"""
    # https://github.com/pydantic/pydantic/issues/3120#issuecomment-1528030416
    return create_model(  # type: ignore
        f'Partial{model.__name__}',
        __base__=model,
        __module__=model.__module__,
        **{
            field_name: make_field_optional(field_info)
            for field_name, field_info in model.model_fields.items()
        }
    )


class JsonLdModel(BaseModel):
    context: Any = Field(alias='@context')
    # TODO: arbitrary type just for context

    def __init__(__pydantic_self__, **data: Any) -> None:
        base = data.get('@base', None)
        ctx = data.get('@context', {})
        context = Context(ctx, base=base)
        __pydantic_self__.__pydantic_validator__.validate_python(
            data,
            self_instance=__pydantic_self__,
            context=dict(ctx=context)
        )
        __pydantic_self__.context = context

    @field_serializer("context")
    def serialize_context(self, value, info):
        # TODO: maybe store the original form?
        # Or send the new URL for the context?
        return value.to_dict() if isinstance(value, Context) else value


class HkSchema(JsonLdModel):
    url: SchemaId = Field(alias='@id')
    eventSchemas: Dict[SchemaName, EventSchema] = Field(default={})
    projectionSchemas: Dict[SchemaName, ProjectionSchema] = Field(default={})

    @field_validator('url')
    @classmethod
    def expand_url(cls, v, info):
        return URIRef(info.context['ctx'].expand(v))

    @field_validator('eventSchemas')
    @classmethod
    def add_event_names(cls, event_schemas, info):
        ctx = info.context['ctx']
        for name, s in event_schemas.items():
            s.name = name
            s.type = URIRef(ctx.expand(name))
        return event_schemas

    @field_validator('projectionSchemas')
    @classmethod
    def add_projection_names(cls, projection_schemas, info):
        ctx = info.context['ctx']
        for name, s in projection_schemas.items():
            s.name = name
            s.type = URIRef(ctx.expand(name))
        return projection_schemas


class EventAttributeSchema(BaseModel):
    name: Name
    range: Union[PydanticURIRef, List[PydanticURIRef]]
    optional: Optional[bool] = True
    create: Optional[bool] = False

    @field_validator('range')
    @classmethod
    def validate_range(cls, v, info):
        if not isinstance(v, list):
            return URIRef(info.context['ctx'].expand(v))
        ranges = [URIRef(info.context['ctx'].expand(u)) for u in v]
        if len(ranges) == 1:
            ranges = ranges[0]
        else:
            assert all(
                x not in scalar_field_types for x in ranges
            ), "No scalar types in range type union"
        return ranges

    @model_validator(mode='after')
    def simple_range_if_creating(self) -> EventAttributeSchema:
        if self.create and isinstance(self.range, list) and len(self.range) > 1:
            raise ValueError('When creating a resource, range cannot be a union')
        return self


class ProjectionAttributeSchema(EventAttributeSchema):
    # What was map_prop again?
    map_prop: Optional[Name] = None
    functional: Optional[bool] = True
    # TODO: indexing

    @field_validator('map_prop')
    @classmethod
    def validate_map_prop(cls, v, info):
        return URIRef(info.context['ctx'].expand(v))


class EventSchema(BaseModel):
    name: Optional[Name] = None
    type: Optional[PydanticURIRef] = None
    attributes: List[EventAttributeSchema]
    restricted: bool = False

    @field_validator('type')
    @classmethod
    def validate_type(cls, v, info):
        return URIRef(info.context['ctx'].expand(v))

class HistoryStorageDirective(Enum):
    no_history = 'no_history'
    separate_history = 'separate_history'
    full_history = 'full_history'
    integrated_history = 'integrated_history'


class ProjectionSchema(EventSchema):
    attributes: List[ProjectionAttributeSchema]
    history_storage: HistoryStorageDirective = HistoryStorageDirective.full_history
    # TODO: snapshot retention policy


class EventHandlerSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    event_type: PydanticURIRef
    target_range: PydanticURIRef
    target_role: str
    code_text: str
    language: str = 'javascript'  # TODO: Make it an enum

    @field_validator('event_type', mode='before')
    @classmethod
    def add_event_type(cls, event_type, info):
        if isinstance(event_type, DeclarativeBase):
            return PydanticURIRef(event_type.uri)
        return PydanticURIRef(event_type) if event_type else None

    @field_validator('target_range', mode='before')
    @classmethod
    def add_target_range(cls, target_range, info):
        if isinstance(target_range, DeclarativeBase):
            return PydanticURIRef(target_range.uri)
        return PydanticURIRef(target_range) if target_range else None


class EventHandlerSchemas(JsonLdModel):
    handlers: List[EventHandlerSchema]


class EntityTopicSchema(BaseModel):
    id: UUID
    projections: List[QName] = []

# TODO: connect to schema


class LangStringModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    value: str = Field(alias='@value')
    language: str = Field(alias='@lang')  # langcodes.Language


class BaseSourceModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: Optional[int] = None
    creator: Optional[str] = None
    public_read: bool = True
    public_write: bool = False
    selective_write: bool = False

    @field_validator('creator', mode='before')
    @classmethod
    def add_creator(cls, creator, info):
        return creator.username if isinstance(creator, DeclarativeBase) else creator


class LocalSourceModel(BaseSourceModel):
    local_name: str
    included_source_ids: Optional[List[int]] = []


class RemoteSourceModel(BaseSourceModel):
    uri: PydanticURIRef


SourceModel = Union[LocalSourceModel, RemoteSourceModel]


class AgentModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    email: str
    username: str
    confirmed: bool = False
    permissions: List[str] = []   # TODO: Enum
    created: Optional[datetime] = None
    last_login: Optional[datetime] = None
    last_login_email_sent: Optional[datetime] = None

    def has_permission(self, permission: str) -> bool:
        return permission in self.permissions or 'admin' in self.permissions


class AgentModelWithPw(AgentModel):
    passwd: str


AgentModelOptional = to_optional(AgentModelWithPw)

class AgentSourcePermissionModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    agent: str
    source: PydanticURIRef
    is_admin: bool = False
    allow_read: bool = False
    allow_write: bool = False
    allow_all_write: bool = False
    is_request: bool = False

    @field_validator('source', mode='before')
    @classmethod
    def add_source(cls, source, info):
        if isinstance(source, DeclarativeBase):
            return PydanticURIRef(source.uri)
        return PydanticURIRef(source) if source else None

    @field_validator('agent', mode='before')
    @classmethod
    def add_creator(cls, agent, info):
        if isinstance(agent, DeclarativeBase):
            return agent.username
        return agent

AgentSourcePermissionModelOptional = to_optional(AgentSourcePermissionModel)

class AgentSourceSelectivePermissionModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    agent: str
    source: PydanticURIRef
    event_type: PydanticURIRef
    is_request: bool = False

    @field_validator('source', mode='before')
    @classmethod
    def add_source(cls, source, info):
        if isinstance(source, DeclarativeBase):
            return PydanticURIRef(source.uri)
        return PydanticURIRef(source) if source else None

    @field_validator('event_type', mode='before')
    @classmethod
    def add_event_type(cls, event_type, info):
        if isinstance(event_type, DeclarativeBase):
            return PydanticURIRef(event_type.uri)
        return PydanticURIRef(event_type) if event_type else None

    @field_validator('agent', mode='before')
    @classmethod
    def add_creator(cls, agent, info):
        if isinstance(agent, DeclarativeBase):
            return agent.username
        return agent

class AgentSourceSelectivePermissionModelOptional(AgentSourceSelectivePermissionModel):
    # event_type should not be optional, as it distinguishes the types
    agent: Optional[str] = None
    source: Optional[PydanticURIRef] = None


HkSchema.model_rebuild()


def read_schema(fname) -> HkSchema:
    with open(fname) as f:
        s = HkSchema.model_validate_json(f.read())
    return s


scalar_field_types = {
    XSD.anyURI: PydanticURIRef,
    XSD.base64Binary: bytes,
    XSD.boolean: bool,
    XSD.date: date,
    XSD.dateTime: datetime,
    XSD.decimal: int,
    XSD.double: float,
    XSD.duration: timedelta,
    XSD.float: float,
    XSD.hexBinary: bytes,
    XSD.gDay: int,
    XSD.gMonth: int,
    XSD.gMonthDay: int,
    XSD.gYear: int,
    XSD.gYearMonth: int,
    # XSD.NOTATION: None,
    XSD.QName: PydanticURIRef,
    XSD.string: str,
    XSD.time: time,
    XSD.language: str,  # langcodes?
    RDF.langString: LangStringModel,
}

validators = {
    XSD.base64Binary: lambda cls, v, info: urlsafe_b64decode(v),
    XSD.hexBinary: lambda cls, v, info: bytes.fromhex(v),
}

def as_field(schema: EventAttributeSchema) -> Field:
    ftype = scalar_field_types.get(as_tuple_or_scalar(schema.range), PydanticURIRef)
    # TODO: defaults
    return (ftype, None)

def validators_for_schema(schema: EventSchema):
    validators = {}
    for ev_schema in schema.attributes:
        if validator := validators.get(as_tuple_or_scalar(ev_schema.range)):
            validators[f'validate_{ev_schema.name}'] = field_validator(ev_schema.name)(validator)
    return validators


EventT = TypeVar('EventT')


class GenericEventModel(BaseModel, Generic[EventT]):
    model_config = ConfigDict(from_attributes=True, extra='forbid')

    source: Optional[PydanticURIRef] = None
    creator: Optional[str] = None
    created: Optional[datetime] = None
    data: EventT

    @field_validator('source', mode='before')
    @classmethod
    def add_source(cls, source, info):
        if isinstance(source, DeclarativeBase):
            return PydanticURIRef(source.uri)
        return PydanticURIRef(source) if source else None

    @field_validator('creator', mode='before')
    @classmethod
    def add_creator(cls, creator, info):
        if isinstance(creator, DeclarativeBase):
            return creator.username
        return creator

    @field_validator('created', mode='before')
    @classmethod
    def add_created(cls, created, info):
        if isinstance(created, str):
            return datetime.fromisoformat((created+'00000')[:26])
        return created

KNOWN_MODELS: Dict[QName: Tuple[EventSchema, BaseModel]] = {}
EVENT_MODEL: type[GenericEventModel] = None


class DynamicBaseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='forbid')


def model_from_schema(schema: EventSchema, prefix: str) -> BaseModel:
    global KNOWN_MODELS, EVENT_MODEL
    attributes = {s.name: as_field(s) for s in schema.attributes}
    attributes['type'] = (Literal[f'{prefix}:{schema.name}'], Field(alias='@type'))
    if isinstance(schema, ProjectionSchema):
        attributes['id'] = (PydanticURIRef, Field(alias='@id'))
        attributes['when'] = (Optional[datetime], Field(None, alias='@when'))
    assert all(list(attributes.values()))
    classname = f'{prefix.title()}{schema.name.title()}'
    validators = validators_for_schema(schema)
    schema_name = f'{prefix}:{schema.name}'
    model = create_model(
        classname,
        __base__=DynamicBaseSchema,  # Only for projections?
        __validators__=validators,
        **attributes)
    KNOWN_MODELS[schema_name] = (schema, model)
    EVENT_MODEL = None
    return model




def models_from_schema(schema: HkSchema) -> Dict[QName, BaseModel]:
    ctx = schema.context
    base = getattr(ctx, 'vocab', None) or getattr(ctx, '_base', None)
    assert base, "The schema needs to have a base"
    prefix = ctx._prefixes[base]
    assert prefix, "The schema base needs to have a prefix"
    return {f'{prefix}:{sschema.name}': model_from_schema(sschema, prefix) for sschema in schema.eventSchemas.values()} | {
        f'{prefix}:{sschema.name}': model_from_schema(sschema, prefix) for sschema in schema.projectionSchemas.values()}

def models_from_schemas(schemas: List[HkSchema]) -> Dict[QName, BaseModel]:
    # Making assumptions about no prefix collisions...
    return reduce(lambda a, b: a | b, [models_from_schema(s) for s in schemas], {})


def getEventModel() -> type[GenericEventModel]:
    global KNOWN_MODELS, EVENT_MODEL
    if EVENT_MODEL is None:
        if types := tuple(m for (s, m) in KNOWN_MODELS.values() if not isinstance(s, ProjectionSchema)):
            class EventModel(GenericEventModel):
                data: Union[types] = Field(discriminator="type")  # type: ignore
            EVENT_MODEL = EventModel
        else:
            EVENT_MODEL = GenericEventModel
    return EVENT_MODEL


def getProjectionSchemas() -> Dict[PydanticURIRef, Tuple[ProjectionSchema, BaseModel]]:
    global KNOWN_MODELS
    return {s.type: (s, m) for (s, m) in KNOWN_MODELS.values() if isinstance(s, ProjectionSchema)}
