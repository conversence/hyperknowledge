from datetime import datetime
from uuid import UUID

from pydantic import AnyUrl, constr
from sqlalchemy.dialects.postgresql import BIGINT

from .pydantic_adapters import PydanticURIRef

Name = constr(pattern=r'\w+')
QName = constr(pattern=r'\w+:\w+')

StreamId = PydanticURIRef
Timestamp = datetime
EntityId = UUID
SchemaId = PydanticURIRef
SchemaName = Name
SchemaElementFull = PydanticURIRef
SchemaElement = QName
UserId = Name
dbTopicId = BIGINT

def as_list(val):
    if isinstance(val, (tuple, list)):
        return val
    return [val]
