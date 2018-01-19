# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
from typing import Any, Type, Union

import enum

def is_common(schema: Any) -> bool: ...

_AnySchema = Union[StreamSchema, CommonSchema, str]

class StreamSchema(object):
    def __init__(self, schema: str) -> None: ...
    @property
    def style(self) -> Type: ...
    def as_tuple(self, named : Union[bool,str]=None) -> 'StreamSchema': ...
    def as_dict(self) -> 'StreamSchema': ...
    def extend(self, schema: Any) -> 'StreamSchema': ...


class CommonSchema(enum.Enum):
    Python: StreamSchema
    Json: StreamSchema
    String: StreamSchema
    Binary: StreamSchema
    XML: StreamSchema
    def extend(self, schema: Any) -> StreamSchema: ...

class _SchemaParser: ...
