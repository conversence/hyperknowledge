"""Make URIRef into a pydantic-friendly type"""

from typing import (
    Any,
)
from collections.abc import Callable
from uuid import UUID

from pydantic_core import core_schema
from typing import Annotated

from rdflib import URIRef

from pydantic import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue


class _URIRefPydanticAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: Callable[[Any], core_schema.CoreSchema],
    ) -> core_schema.CoreSchema:
        """
        We return a pydantic_core.CoreSchema that behaves in the following ways:

        * Strings will be parsed as `URIRef` instances with the int as the x attribute
        * `URIRef` instances will be parsed as `URIRef` instances without any changes
        * Nothing else will pass validation
        * Serialization will always return just a string
        """

        def validate_from_str(value: str) -> URIRef:
            if value.startswith("urn:uuid:"):
                # Check validity
                UUID(value[9:])
            return URIRef(value)

        from_str_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(validate_from_str),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=from_str_schema,
            python_schema=core_schema.union_schema(
                [
                    # check if it's an instance first before doing any further work
                    core_schema.is_instance_schema(URIRef),
                    from_str_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda instance: str(instance)
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, _core_schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        # Use the same schema that would be used for `int`
        return handler(core_schema.str_schema())


PydanticURIRef = Annotated[URIRef, _URIRefPydanticAnnotation]
