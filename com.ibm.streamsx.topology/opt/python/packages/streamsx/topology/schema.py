# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
"""
Schemas for streams.

On a structured stream a tuple is a sequence of attributes,
and an attribute is a named value of a specific type.

The supported types are defined by IBM Streams Streams Processing Language (SPL).
"""
from __future__ import (absolute_import, division,
                        print_function, unicode_literals)
from builtins import *

import collections
import enum
import io
import token
import tokenize


# Parses a schema of the form 'tuple<...>'
# _parse returns a list of the schema attributes,
# each attribute is a python tuple of:
# (type, name)
# with type being
#    primitive type (str), e.g. 'int32'
#    collection type (tuple), e.g. ('list', 'int32')
#    nested tuple type (tuple), e.g. ('tuple', [('int32', 'a'), ('float64', 'b')])
# This is an internal api.
#
class _SchemaParser(object):
    """Class for parsing schemas."""
    _SPL_PRIMITIVE_TYPES = { 'boolean',
                         'uint8', 'uint16', 'uint32', 'uint64',
                         'int8', 'int16', 'int32', 'int64',
                         'float32', 'float64',
                         'complex32', 'complex64',
                         'rstring', 'ustring',
                         'timestamp', 'blob', 'xml'}

    _SPL_COLLECTION_TYPES = { 'list', 'set'}

    def __init__(self, schema):
        self.schema = schema
        self._type = []

    def _parse_error(self, token):
        raise SyntaxError("Invalid schema:" + self.schema + " token " + str(token))

    def _req_op(self, tokens, which):
        token = next(tokens)
        if token[0] != tokenize.OP or which != token[1]:
            self._parse_error(token)

    def _parse(self):
        schema = self.schema.replace(">>", ' > > ')
        schema = schema.replace('<<', ' < < ')

        ios = io.StringIO(schema).readline
        tokens = tokenize.generate_tokens(ios)
        self._parse_tuple(self._type, next(tokens), tokens)
        endtoken = next(tokens)
        if not endtoken[0] == token.ENDMARKER:
            self._parse_error(endtoken)
        return self._type

    def _parse_tuple(self, _type, token, tokens):
        if token[0] != tokenize.NAME or 'tuple' != token[1]:
            self._parse_error(token)
        self._req_op(tokens, '<')
    
        token = None
        while True:
            token = next(tokens)
            if token[0] == tokenize.OP:
                if token[1] == ',':
                    continue
                if token[1] == '>':
                    break
                self._parse_error(token)

            if token[0] == tokenize.NAME:
                self._parse_attribute_type(_type, token, tokens)
                continue

            self._parse_error(token)

    def _parse_type(self, attr_type, tokens):
        if attr_type[0] != tokenize.NAME:
            self._parse_error(attr_type)

        if 'tuple' == attr_type[1]:
            nested_tuple = []
            self._parse_tuple(nested_tuple, attr_type, tokens)
            return ('tuple', nested_tuple)

        if 'map' == attr_type[1]:
            self._req_op(tokens, '<')
            key_type = self._parse_type(next(tokens), tokens)
            self._req_op(tokens, ',')
            value_type = self._parse_type(next(tokens), tokens)
            self._req_op(tokens, '>')
            return ('map', (key_type, value_type))
            
        if attr_type[1] in _SchemaParser._SPL_PRIMITIVE_TYPES:
            return attr_type[1]

        if attr_type[1] in _SchemaParser._SPL_COLLECTION_TYPES:
            self._req_op(tokens, '<')
            element_type = self._parse_type(next(tokens), tokens)
            self._req_op(tokens, '>')
            return (attr_type[1], element_type)

        self._parse_error(attr_type)

    def _parse_attribute_type(self, _type, attr_type, tokens):
        if attr_type[0] != tokenize.NAME:
            self._parse_error(attr_type)

        attr_type = self._parse_type(attr_type, tokens)

        attr = (attr_type, self._parse_attribute_name(tokens))
        _type.append(attr)

    def _parse_attribute_name(self, tokens):
        attr_name = next(tokens)
        if attr_name[0] != tokenize.NAME:
            self._parse_error(attr_name)
        return attr_name[1]


def _stream_schema(schema):
    if isinstance(schema, StreamSchema):
        return schema
    if isinstance(schema, CommonSchema):
        return schema
    return StreamSchema(str(schema))

def _attribute_names(types):
    names = []
    for attr in types:
        names.append(attr[1])
    return names

class StreamSchema(object) :
    """Defines a schema for a structured stream.

    On a structured stream a tuple is a sequence of attributes,
    and an attribute is a named value of a specific type.

    The supported types are defined by IBM Streams Streams Processing
    Language and include such types as `int8`, `int16`, `rstring`
    and `list<float32>`.

    A schema is defined with the syntax ``tuple<type name [,...]>``,
    for example::

        tuple<rstring id, timestamp ts, float64 value>

    represents a schema with three attributes suitable for a sensor reading.

    A `StreamSchema` can be created by passing a string of the
    for ``tuple<...>`` or by passing the name of an SPL type from
    an SPL toolkit, for example ``com.ibm.streamsx.transportation.vehicle::VehicleLocation``.

    Attribute names must start with an ASCII letter or underscore, followed by ASCII letters, digits, or underscores.

    When a tuple on a structured scheme is passed into Python it
    is converted to a `dict` containing all attributes of the tuple.
    Each key is the attribute name as a `str` and
    the value is the attribute's value.

    When a Python object is submitted to a structured stream,
    for example as the return from the function invoked in a 
    :py:meth:`~streamsx.topology.topology.Stream.map` with the
    `schema` parameter set, it must be:
         * A Python tuple. Attributes are set by position, with the first attribute being the value at index 0 in the Python tuple. If a value does not exist (the tuple has less values than the structured schema) or is set to `None` then the attribute has its default value, zero, false, empty list or string etc.

    Args:
        schema(str): Schema definition. Either a schema definition or the name of an SPL type.
    """
    def __init__(self, schema):
        schema = schema.strip()
        self.__spl_type = not schema.startswith("tuple<")
        self.__schema=schema
        self.__nt = None
        if not self.__spl_type:
            parser = _SchemaParser(schema)
            self._types = parser._parse()
            
    def _set(self, schema):
        """Set a schema from another schema"""
        if isinstance(schema, CommonSchema):
            self.__spl_type = False
            self.__schema = schema.schema()
        else:
            self.__spl_type = schema.__spl_type
            self.__schema = schema.__schema

    def schema(self):
        """Private method. May be removed at any time."""
        return self.__schema

    def __str__(self):
        """Private method. May be removed at any time."""
        return self.__schema

    def spl_json(self):
        """Private method. May be removed at any time."""
        _splj = {}
        _splj["type"] = 'spltype'
        _splj["value"] = self.schema()
        return _splj

    def extend(self, schema):
        """
        Extend a structured schema by another.

        For example extending ``tuple<rstring id, timestamp ts, float64 value>``
        with ``tuple<float32 score>`` results in ``tuple<rstring id, timestamp ts, float64 value, float32 score>``.

        Args:
            schema(StreamSchema): Schema to extend this schema by.

        Returns:
            StreamSchema: New schema that is an extension of this schema.
        """
        if self.__spl_type:
           raise TypeError("Not supported for declared SPL types")
        base = self.schema()
        extends = schema.schema()
        new_schema = base[:-1] + ',' + extends[6:]
        return StreamSchema(new_schema)

    def __hash__(self):
        return hash(self.schema())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.schema() == other.schema()
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    _NAMED_SCHEMAS = {}

    def _namedtuple(self):
        """WIP - Gets a named tuple that matches the schema."""
        if self.__nt is not None:
            return self.__nt
        if self in StreamSchema._NAMED_SCHEMAS:
             return StreamSchema._NAMED_SCHEMAS[self]

        name = "Structured"
        self.__nt = collections.namedtuple(name, _attribute_names(self._types))
        StreamSchema._NAMED_SCHEMAS[self] = self.__nt
        return self.__nt

@enum.unique
class CommonSchema(enum.Enum):
    """
    Common stream schemas for interoperability within Streams applications.

    Streams application can publish streams that are subscribed to by other applications.
    Use of common schemas allow streams connections regardless of the application implementation language.

    Python applications publish streams using :py:meth:`~streamsx.topology.topology.Stream.publish`
    and subscribe using :py:meth:`~streamsx.topology.topology.Topology.subscribe`.
    
     * :py:const:`Python` - Stream constains Python objects.
     * :py:const:`Json` - Stream contains JSON objects.
     * :py:const:`String` - Stream contains strings.
     * :py:const:`Binary` - Stream contains binary tuples.
     * :py:const:`XML` - Stream contains XML documents.
    """
    Python = StreamSchema("tuple<blob __spl_po>")
    """
    Stream where each tuple is a Python object. Each object
    must be picklable to allow execution in a distributed
    environment where streams can connect processes
    running on the same or different resources.

    Python streams can only be used by Python applications.
    """
    Json = StreamSchema("tuple<rstring jsonString>")
    """
    Stream where each tuple is logically a JSON object.

    `Json` can be used as a natural interchange format between Streams applications
    implemented in different programming languages. All languages supported by
    Streams support publishing and subscribing to JSON streams.

    A Python callable receives each tuple as a `dict` as though it was
    created from ``json.loads(json_formatted_str)`` where `json_formatted_str`
    is the JSON formatted representation of tuple.

    Python objects that are to be converted to JSON objects
    must be supported by `JSONEncoder`. If the object is not a `dict`
    then it will be converted to a JSON object with a single key `payload`
    containing the value.
    """
    String = StreamSchema("tuple<rstring string>")
    """
    Stream where each tuple is a string.

    `String` can be used as a natural interchange format between Streams applications
    implemented in different programming languages. All languages supported by
    Streams support publishing and subscribing to string streams.

    A Python callable receives each tuple as a `str` object.

    Python objects are converted to strings using ``str(obj)``.
    """
    Binary = StreamSchema("tuple<blob binary>")
    """
    Stream where each tuple is a binary object (sequence of bytes).

    .. warning:: `Binary` is not yet supported for Python applications.
    """
    XML = StreamSchema("tuple<xml document>")
    """
    Stream where each tuple is an XML document.

    .. warning:: `XML` is not yet supported for Python applications.
    """

    def schema(self):
        """Private method. May be removed at any time."""
        return self.value.schema()

    def spl_json(self):
        """Private method. May be removed at any time."""
        return self.value.spl_json()

    def extend(self, schema):
        """Extend a structured schema by another.

        Args:
            schema(StreamSchema): Schema to extend this schema by.

        Returns:
            StreamSchema: New schema that is an extension of this schema.
        """
        return self.value.extend(schema)

    def __str__(self):
        return str(self.schema())

