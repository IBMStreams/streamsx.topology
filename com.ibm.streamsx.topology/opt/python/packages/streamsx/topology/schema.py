# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2019
"""
Schemas for streams.

********
Overview
********

A stream represents an unbounded flow of tuples with a declared schema so that each tuple on the stream complies with the schema. A stream's schema may be one of:

    * :py:class:`~StreamsSchema` structured schema - a tuple is a sequence of attributes, and an attribute is a named value of a specific type.
    * :py:const:`~CommonSchema.Json` a tuple is a JSON object.
    * :py:const:`~CommonSchema.String` a tuple is a string.
    * :py:const:`~CommonSchema.Python` a tuple is any Python object, effectively an untyped stream.

******************
Structured schemas
******************

A structured schema is a sequence of attributes, and an attribute is a named value of a specific type. For example a stream of sensor readings can be represented as a schema with three attributes ``sensor_id``, ``ts`` and ``reading`` with types of ``int64``, ``int64`` and ``float64`` respectively.

This schema can be declared a number of ways:

Python 3.6::

    class SensorReading(typing.NamedTuple):
        sensor_id: int
        ts: int
        reading: float

    sensors = raw_readings.map(parse_sensor, schema=SensorReading)
    
Python 3::

    sensors = raw_readings.map(parse_sensor,
        schema='tuple<int64 sensor_id, int64 ts, float64 reading>')

The supported types are defined by IBM Streams and are listed in :py:class:`~StreamSchema`.

*************************
Nested structured schemas
*************************

A structured schema can contain nested structures that are defined separately.

Python 3.6::

    class Sensor(typing.NamedTuple):
        manufacturer: str
        sensor_id: int

    class SensorReading(typing.NamedTuple):
        sensor: Sensor
        ts: int
        reading: float

    sensors = raw_readings.map(parse_sensor, schema=SensorReading)
    
Python 3::

    sensors = raw_readings.map(parse_sensor,
        schema='tuple<tuple<rstring manufacturer, int64 sensor_id> sensor, int64 ts, float64 reading>')

Both schema definitions are equivalent.


Structured schemas provide type-safety and efficient network serialization when compared to passing a ``dict`` using :py:const:`~CommonSchema.Python` streams. 

Streams with structured schemas can be interchanged with any IBM Streams application using :py:meth:`~streamsx.topology.topology.Stream.publish` and :py:meth:`~streamsx.topology.topology.Topology.subscribe` maintaining type safety.

**************************
Defining a stream's schema
**************************

Every stream within a :py:class:`~streamsx.topology.topology.Topology` has defined schema. The schema may be defined explictly (for example :py:meth:`~streamsx.topology.topology.Stream.map` or :py:meth:`~streamsx.topology.topology.Topology.subscribe`) or implicity (for example :py:meth:`~streamsx.topology.topology.Stream.filter` produces a stream with the same schema as its input stream).

Explictly defining a stream's schema is flexible and various types of values are accepted as the schema.

    * Builtin types as aliases for common schema types:

        * ``json`` (module) - for  :py:const:`~CommonSchema.Json`
        * ``str`` - for  :py:const:`~CommonSchema.String`
        * ``object`` - for :py:const:`~CommonSchema.Python`

    * Values of the enumeration :py:class:`CommonSchema`
    * An instance of ``typing.NamedTuple`` (Python 3)
    * An instance of :py:class:`StreamSchema`
    * A string of the format ``tuple<...>`` defining the attribute names and types. See :py:class:`StreamSchema` for details on the format and types supported.
    * A string containing a namespace qualified SPL stream type (e.g. ``com.ibm.streams.geospatial::FlightPathEncounterTypes.Observation3D``)

"""

# For style dicts passed into Python from Streams C++
# are raw dicts since they are created by Python C-API code
# not the future dict in Python 2.7.
_spl_dict = dict
_spl_object = object

__all__ = ['is_common', 'StreamSchema', 'CommonSchema']

import collections
import decimal
import datetime
import enum
import io
import itertools
import json
import sys
import token
import tokenize
import typing

import streamsx._streams._version
__version__ = streamsx._streams._version.__version__


_spl_str = str

def _normalize(schema, allow_none=True):
    """
    Normalize a schema.
    """
    if allow_none and schema is None:
        return schema
    if isinstance(schema, CommonSchema):
        return schema
    if isinstance(schema, StreamSchema):
        return schema

    if isinstance(schema, str):
        return StreamSchema(schema)

    py_types = {
        _spl_object: CommonSchema.Python,
        _spl_str: CommonSchema.String,
        json: CommonSchema.Json,
        }

    if schema in py_types:
        return py_types[schema]

    # Allow a named tuple with type hints
    # to be used as a schema definition
    if _is_typedtuple(schema):
        return _from_named_tuple(schema)

    raise ValueError("Unknown stream schema type:" + str(schema))

def is_common(schema):
    """
    Is `schema` an common schema.

    Args:
        schema: Scheme to test.

    Returns:
        bool: ``True`` if schema is a common schema, otherwise ``False``.

    """
    if isinstance(schema, StreamSchema):
        return schema.schema() in _SCHEMA_COMMON
    if isinstance(schema, CommonSchema):
        return True
    if isinstance(schema, str):
        return is_common(StreamSchema(schema))
    return False

def _is_namedtuple(cls):
    return cls != tuple and isinstance(cls, type) and issubclass(cls, tuple) and hasattr(cls, '_fields')

def _is_typedtuple(cls):
    return _is_namedtuple(cls) and hasattr(cls, '_field_types')

_SCHEMA_PENDING = '<pending>'
def _is_pending(schema):
    return isinstance(schema, StreamSchema) and schema.schema() == _SCHEMA_PENDING

# Parses a schema of the form 'tuple<...>'
# _parse returns a list of the schema attributes,
# each attribute is a python tuple of:
# (type, name)
# with type being
#    primitive type (str), e.g. 'int32'
#    collection type (tuple), e.g. ('list', 'int32')
#    optional type (tuple), e.g. ('optional', 'int32')
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
                         'decimal32', 'decimal64', 'decimal128',
                         'rstring', 'ustring',
                         'timestamp', 'blob', 'xml'}

    _SPL_COLLECTION_TYPES = { 'list', 'set'}

    def __init__(self, schema):
        self.schema = schema
        self._type = []

    def _parse_error(self, token):
        raise SyntaxError("Invalid schema:" + self.schema + " token " + str(token))

    def _req_op(self, which):
        token = next(self.tokens)
        if token[0] != tokenize.OP or which != token[1]:
            self._parse_error(token)

    def _parse(self):
        schema = self.schema.replace(">>", ' > > ')
        schema = schema.replace('<<', ' < < ')

        ios = io.StringIO(schema).readline
        self.tokens = tokenize.generate_tokens(ios)
        self._parse_tuple(self._type, next(self.tokens))
        endtoken = next(self.tokens)
        # Python 3.6.7 adds a newline at the end! Issue #1959
        if endtoken[0] == token.NEWLINE:
            endtoken = next(self.tokens)
        if not endtoken[0] == token.ENDMARKER:
            self._parse_error(endtoken)
        return self._type

    def _parse_tuple(self, _type, token):
        if token[0] != tokenize.NAME or 'tuple' != token[1]:
            self._parse_error(token)
        self._req_op('<')
    
        token = None
        while True:
            token = next(self.tokens)
            if token[0] == tokenize.OP:
                if token[1] == ',':
                    continue
                if token[1] == '>':
                    break
                self._parse_error(token)

            if token[0] == tokenize.NAME:
                self._parse_attribute_type(_type, token)
                continue

            self._parse_error(token)

    def _parse_type(self, attr_type):
        if attr_type[0] != tokenize.NAME:
            self._parse_error(attr_type)

        if 'tuple' == attr_type[1]:
            nested_tuple = []
            self._parse_tuple(nested_tuple, attr_type)
            return ('tuple', nested_tuple)

        if 'map' == attr_type[1]:
            self._req_op('<')
            key_type = self._parse_type(next(self.tokens))
            self._req_op(',')
            value_type = self._parse_type(next(self.tokens))
            self._req_op('>')
            bound = self._parse_optional_bounded()
            return ('map', (key_type, value_type), bound)

        if 'optional' == attr_type[1]:
            self._req_op('<')
            value_type = self._parse_type(next(self.tokens))
            self._req_op('>')
            return ('optional', value_type)
            
        if attr_type[1] in _SchemaParser._SPL_PRIMITIVE_TYPES:
            if attr_type[1] == 'rstring':
                bound = self._parse_optional_bounded()
                if bound is not None:
                    return 'rstring' + bound
            return attr_type[1]

        if attr_type[1] in _SchemaParser._SPL_COLLECTION_TYPES:
            self._req_op('<')
            element_type = self._parse_type(next(self.tokens))
            self._req_op('>')
            bound = self._parse_optional_bounded()
            return (attr_type[1], element_type, bound)

        self._parse_error(attr_type)

    def _parse_attribute_type(self, _type, attr_type):
        if attr_type[0] != tokenize.NAME:
            self._parse_error(attr_type)

        attr_type = self._parse_type(attr_type)

        attr = (attr_type, self._parse_attribute_name())
        _type.append(attr)

    def _parse_attribute_name(self):
        attr_name = next(self.tokens)
        if attr_name[0] != tokenize.NAME:
            self._parse_error(attr_name)
        return attr_name[1]

    def _parse_optional_bounded(self):
        token = next(self.tokens)
        if token[0] == tokenize.OP and '[' == token[1]:
            bound_info = next(self.tokens)
            if bound_info[0] != tokenize.NUMBER:
                self._parse_error(bound_info)
            bound = str(int(bound_info[0]))
            self._req_op(']')
            return bound
        else:
            # push back the token
            self.tokens = itertools.chain([token], self.tokens)
            return None

def _attribute_names(types):
    names = []
    for attr in types:
        names.append(attr[1])
    return names

# Returns a schema's types as a list of (name, python type) tuples
# suitable for use in creating a typing.NamedTuple
def _attribute_pytypes(types):
    pytypes = []
    for attr in types: 
        pytypes.append((attr[1], _type_from_spl(attr[0])))
    return pytypes

_SCHEMA_PYTHON_OBJECT = 'tuple<blob __spl_po>'
_SCHEMA_STRING = 'tuple<rstring string>'
_SCHEMA_JSON = 'tuple<rstring jsonString>'
_SCHEMA_BINARY = 'tuple<blob binary>' # not yet supported
_SCHEMA_XML = 'tuple<xml document>' # not yet supported

_SCHEMA_COMMON = frozenset([_SCHEMA_PYTHON_OBJECT, _SCHEMA_JSON, _SCHEMA_STRING, _SCHEMA_BINARY, _SCHEMA_XML])

_SCHEMA_COMMON_STYLES = {_SCHEMA_PYTHON_OBJECT:_spl_object, _SCHEMA_STRING: _spl_str, _SCHEMA_JSON: _spl_dict, _SCHEMA_BINARY:None, _SCHEMA_XML: None }

class StreamSchema(object) :
    """Defines a schema for a structured stream.

    On a structured stream a tuple is a sequence of attributes,
    and an attribute is a named value of a specific type.

    The supported types are defined by IBM Streams and include such
    types as `int8`, `int16`, `rstring` and `list<float32>`.

    A structured schema can be defined using a ``typing.NamedTuple`` in
    Python 3, a string with the syntax ``tuple<type name [,...]>`` or
    an instance of this class.

    typing.NamedTuple:
 
        A ``typing.NamedTuple`` can be used to define a structured
        schema with the field names and types mapping to the
        structured schema attribute names and types.

        Python types are mapped to IBM Streams types as follows:

        ================================== ================
        Python type                        IBM Streams type
        ================================== ================
        ``str``                            ``rstring``
        ``bool``                           ``boolean``
        ``int``                            ``int64``
        ``float``                          ``float64``
        ``decimal.Decimal``                ``decimal128``
        ``complex``                        ``complex64``
        ``bytes``                          ``blob``
        ``streamsx.spl.types.Timestamp``   ``timestamp``
        ``datetime.datetime``              ``timestamp``
        ``typing.List[T]``                 ``list<T>``
        ``typing.Set[T]``                  ``set<T>``
        ``typing.Mapping[K,V]``            ``map<K,V>``
        ``typing.Optional[T]``             ``optional<T>``
        ================================== ================

        .. note::
            Tuples on a stream with a schema defined by a
            ``typing.NamedTuple`` instance are passed into callables
            as instance of a named tuple with the the correct field
            names and types unless the named tuple contains nested
            named tuples at any nesting depth. When passed as named tuple,
            there is no guarantee to be the same class
            instance as the one used to declare the schema.

    Tuple string:

        A string of the format `tuple<type name [,...]>` can be used
        to define a structured schema, where `type` is an IBM Streams type.

        Example::

            tuple<rstring id, timestamp ts, float64 value>

        represents a schema with three attributes suitable for a sensor reading.

    IBM Streams types:

    ============================  ==============================  =========================================  =======================================
    Type                          Description                     Python representation                      Conversion from Python
    ============================  ==============================  =========================================  =======================================
    ``boolean``                   True or False                   ``bool``                                   ``bool(value)``
    ``int8``                      8-bit signed integer            ``int``                                    ``int(value)`` truncated to 8 bits
    ``int16``                     16-bit signed integer           ``int``                                    ``int(value)`` truncated to 16 bits
    ``int32``                     32-bit signed integer           ``int``                                    ``int(value)`` truncated to 32 bits
    ``int64``                     64-bit signed integer           ``int``                                    ``int(value)``
    ``uint8``                     8-bit unsigned integer          ``int``                                    -
    ``uint16``                    16-bit unsigned integer         ``int``                                    -
    ``uint32``                    32-bit unsigned integer         ``int``                                    -
    ``uint64``                    64-bit unsigned integer         ``int``                                    -
    ``float32``                   32-bit binary floating point    ``float``                                  ``float(value)`` truncated to 32 bits
    ``float64``                   64-bit binary floating point    ``float``                                  ``float(value)``
    ``decimal32``                 32-bit decimal floating point   ``decimal.Decimal``                        ``decimal.Decimal(value)`` normalized to IEEE 754 decimal32
    ``decimal64``                 64-bit decimal floating point   ``decimal.Decimal``                        ``decimal.Decimal(value)`` normalized to IEEE 754 decimal64
    ``decimal128``                128-bit decimal floating point  ``decimal.Decimal``                        ``decimal.Decimal(value)`` normalized to IEEE 754 decimal128
    ``complex32``                 complex with `float32` values   ``complex``                                ``complex(value)`` with real and imaginary values truncated to 32 bits
    ``complex64``                 complex with `float64` values   ``complex``                                ``complex(value)``
    ``timestamp``                 Nanosecond timestamp            :py:class:`~streamsx.spl.types.Timestamp`  -
    ``rstring``                   UTF-8 string                    ``str``                                    ``str(value)``
    ``rstring[N]``                Bounded UTF-8 string            ``str``                                    ``str(value)``
    ``ustring``                   UTF-16 string                   ``str``                                    ``str(value)``
    ``blob``                      Sequence of bytes               ``memoryview``                             -
    ``list<T>``                   List with elements of type `T`  ``list``                                   -
    ``list<T>[N]``                Bounded list                    ``list``                                   -
    ``set<T>``                    Set with elements of type `T`   ``set``                                    -
    ``set<T>[N]``                 Bounded set                     ``set``                                    -
    ``map<K,V>``                  Map with typed keys and values  ``dict``                                   -
    ``map<K,V>[N]``               Bounded map, limted to N pairs  ``dict``                                   -
    ``optional<T>``               Optional value of type `T`      Value of type `T`, or None                 Value of for type ``T``
    ``enum{id [,...]}``           Enumeration                     Not supported                              Not supported
    ``xml``                       XML value                       Not supported                              Not supported
    ``tuple<type name [, ...]>``  Nested tuple                    ``dict``                                   ``dict``
    ============================  ==============================  =========================================  =======================================

    .. note::
        Type `optional<T>` requires IBM Streams 4.3 or later.

    .. note::

        Conversion to or from Python:

        * Type `set<T>` is restricted to primitive types

        * Type `map<K,V>` is restricted to primitive types for the key type K

    Python representation is how an attribute value in a structured schema is passed into a Python function. 

    Conversion from Python indicates how a value from Python is converted to an attribute value in a structured schema.
    For example a value ``v`` assigned to ``float64`` attribute is converted as though ``float(v)`` is called first,
    thus ``v`` may be a ``float``, ``int`` or any type that has a ``__float__`` method.
    
    When a type is not supported in Python it can only be used in a schema used for streams produced and consumed by invocation of SPL operators.

    A `StreamSchema` can be created by passing a string of the
    form ``tuple<...>`` or by passing the name of an SPL type from
    an SPL toolkit, for example ``com.ibm.streamsx.transportation.vehicle::VehicleLocation``.

    Attribute names must start with an ASCII letter or underscore, followed by ASCII letters, digits, or underscores.

    When a tuple on a structured stream is passed into a Python callable it
    is converted to a ``dict``, ``tuple`` or **named tuple** object containing all attributes of the stream tuple.
    See :py:meth:`style`, :py:meth:`as_dict` and :py:meth:`as_tuple` for details.

    .. note::
        When a tuple on a structured stream, that contains **nested tuples**, is passed into a Python callable it is **always** converted to a ``dict`` object containing all attributes of the stream tuple.

    When a Python object is submitted to a structured stream,
    for example as the return from the function invoked in a 
    :py:meth:`~streamsx.topology.topology.Stream.map` with the
    `schema` parameter set, it must be:

         * A Python ``dict``. Attributes are set by name using value in the dict for the name. If a value does not exist (the name does not exist as a key) or is set to `None` then the attribute has its default value, zero, false, empty list or string etc.
         * A Python ``tuple`` or named tuple. Attributes are set by position, with the first attribute being the value at index 0 in the Python `tuple`. If a value does not exist (the tuple has less values than the structured schema) or is set to `None` then the attribute has its default value, zero, false, empty list or string etc.

    Args:
        schema(str): Schema definition. Either a schema definition or the name of an SPL type.

    .. versionadded:: 1.16 Support for nested tuples (conversion to SPL from Python or conversion to Python from SPL)
    """
    def __init__(self, schema):
        schema = schema.strip()
        self._spl_type = not schema.startswith("tuple<")
        self._schema=schema
        if not self._spl_type:
            parser = _SchemaParser(schema)
            self._types = parser._parse()

        self._style = self._default_style()
            
    def _set(self, schema):
        """Set a schema from another schema"""
        if isinstance(schema, CommonSchema):
            self._spl_type = False
            self._schema = schema.schema()
            self._style = self._default_style()
        else:
            self._spl_type = schema._spl_type
            self._schema = schema._schema
            self._style = schema._style

    @property
    def style(self):
        """Style stream tuples will be passed into a callable.

        For the common schemas the style is fixed:

            * ``CommonSchema.Python`` - ``object`` - Stream tuples are arbitrary objects.
            * ``CommonSchema.String`` - ``str`` - Stream tuples are unicode strings.
            * ``CommonSchema.Json`` - ``dict`` - Stream tuples are a ``dict`` that represents the JSON object.

        For a structured schema the supported styles are:

            * ``dict`` - Stream tuples are passed as a ``dict`` with the key being the attribute name and and the value the attribute value. This is the default.

                * E.g. with a schema of ``tuple<rstring id, float32 value>`` a value is passed as ``{'id':'TempSensor', 'value':20.3}``.

            * ``tuple`` - Stream tuples are passed as a ``tuple`` with the value being the attributes value in order. A schema is set to pass stream tuples as tuples using :py:meth:`as_tuple`.

                * E.g. with a schema of ``tuple<rstring id, float32 value>`` a value is passed as ``('TempSensor', 20.3)``.

            * ``namedtuple`` - Stream tuples are passed as a named tuple (see ``collections.namedtuple``) with the value being the attributes value in order. Field names correspond to the attribute names of the schema. A schema is set to pass stream tuples as named tuples using :py:meth:`as_tuple` setting the `named` parameter.

        Returns:
            type: Class of tuples that will be passed into callables.

        .. versionadded:: 1.8
        .. versionadded:: 1.9 Support for namedtuple.
        """
        return self._style

    def _default_style(self):
        if self._spl_type:
            return _spl_dict
        return _SCHEMA_COMMON_STYLES[self.schema()] if is_common(self) else _spl_dict

    def _copy(self, style=None):
        if style is None:
            return self
        if self._style is style:
            return self
        # Cannot change style of common schemas
        if is_common(self):
            return self
        c = StreamSchema(self.schema())
        c._style = style
        return c

    def _make_named_tuple(self, name):
        if self._spl_type:
            return tuple
        if name is True:
            name = 'StreamTuple'
        nt = typing.NamedTuple(name, _attribute_pytypes(self._types))

        nt._splpy_namedtuple = name
        return nt

    def as_tuple(self, named=None):
        """
        Create a structured schema that will pass stream tuples into callables as ``tuple`` instances.

        If this instance represents a common schema then it will be returned
        without modification. Stream tuples with common schemas are
        always passed according to their definition.

        **Passing as tuple**

        When `named` evaluates to ``False`` then each stream tuple will
        be passed as a ``tuple``. For example with a structured schema
        of ``tuple<rstring id, float64 value>`` a value is passed as
        ``('TempSensor', 27.4)`` and access to the first attribute
        is ``t[0]`` and the second as ``t[1]`` where ``t`` represents
        the passed value..

        **Passing as named tuple**

        When `named` is ``True`` or a ``str`` then each stream tuple will
        be passed as a named tuple. For example with a structured schema
        of ``tuple<rstring id, float64 value>`` a value is passed as
        ``('TempSensor', 27.4)`` and access to the first attribute
        is ``t.id`` (or ``t[0]``) and the second as ``t.value`` (``t[1]``)
        where ``t`` represents the passed value.

        .. warning:: If an schema's attribute name is not a valid Python identifier or
            starts with an underscore then it will be renamed as positional name ``_n``.
            For example, with the schema ``tuple<int32 a, int32 def, int32 id>`` the
            field names are ``a``, ``_1``, ``_2``.


        The value of `named` is used as the name of the named tuple
        class with ``StreamTuple`` used when `named` is ``True``.

        It is not guaranteed that the class of the namedtuple is the
        same for all callables processing tuples with the same
        structured schema, only that the tuple is a named tuple
        with the correct field names.

        Args:
            named: Pass stream tuples as a named tuple.
                If not set then stream tuples are passed as
                instances of ``tuple``.

        Returns:
            StreamSchema: Schema passing stream tuples as ``tuple`` if allowed.

        .. versionadded:: 1.8
        .. versionadded:: 1.9 Addition of `named` parameter.
        """
        if not named:
            return self._copy(tuple)

        if named == True or isinstance(named, str):
            return self._copy(self._make_named_tuple(name=named))

        return self._copy(tuple)

    def as_dict(self):
        """
        Create a structured schema that will pass stream tuples into callables as ``dict`` instances.
        This allows a return to the default calling style for a structured schema.

        If this instance represents a common schema then it will be returned
        without modification. Stream tuples with common schemas are always passed according
        to their definition.

        Returns:
            StreamSchema: Schema passing stream tuples as ``dict`` if allowed.

        .. versionadded:: 1.8
        """
        return self._copy(_spl_dict)

    def schema(self):
        """Private method. May be removed at any time."""
        return self._schema

    def __str__(self):
        """Private method. May be removed at any time."""
        return self._schema

    def spl_json(self):
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
        if self._spl_type:
           raise TypeError("Not supported for declared SPL types")
        base = self.schema()
        extends = schema.schema()
        new_schema = base[:-1] + ',' + extends[6:]
        return StreamSchema(new_schema)

    def __hash__(self):
        return hash(self.schema())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.schema() == other.schema():
                if self.style == other.style:
                    return True
                if _is_typedtuple(self.style) and _is_typedtuple(other.style):
                    return self.style._fields == other.style._fields and self.style._field_types == other.style._field_types
 
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @staticmethod
    def _fnop_style(schema, op, name):
        """Set an operator's parameter representing the style of this schema."""
        if is_common(schema):
            if name in op.params:
                del op.params[name]
            return
        if _is_pending(schema):
            ntp = 'pending'
        elif schema.style is tuple:
            ntp = 'tuple'
        elif schema.style is _spl_dict:
            ntp = 'dict'
        elif _is_namedtuple(schema.style) and hasattr(schema.style, '_splpy_namedtuple'):
            ntp = 'namedtuple:' + schema.style._splpy_namedtuple
        else:
            return
        op.params[name] = ntp


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
    Python = StreamSchema(_SCHEMA_PYTHON_OBJECT)
    """
    Stream where each tuple is a Python object. Each object
    must be picklable to allow execution in a distributed
    environment where streams can connect processes
    running on the same or different resources.

    Python streams can only be used by Python applications.
    """
    Json = StreamSchema(_SCHEMA_JSON)
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
    String = StreamSchema(_SCHEMA_STRING)
    """
    Stream where each tuple is a string.

    `String` can be used as a natural interchange format between Streams applications
    implemented in different programming languages. All languages supported by
    Streams support publishing and subscribing to string streams.

    A Python callable receives each tuple as a `str` object.

    Python objects are converted to strings using ``str(obj)``.
    """
    Binary = StreamSchema(_SCHEMA_BINARY)
    """
    Stream where each tuple is a binary object (sequence of bytes).

    .. warning:: `Binary` is not yet supported for Python applications.
    """
    XML = StreamSchema(_SCHEMA_XML)
    """
    Stream where each tuple is an XML document.

    .. warning:: `XML` is not yet supported for Python applications.
    """

    def schema(self):
        """Private method. May be removed at any time."""
        return self.value.schema()

    def spl_json(self):
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

def _from_named_tuple(nt):
    nested = 0

    i = 0
    td = 'tuple<'    
    for name in nt._fields:
        if i:
            td += ', '
        typeval = _spl_from_type(nt._field_types[name])
        # special handling for nested tuple types
        if typeval.startswith('tuple') or typeval.startswith('list<tuple'): # __NESTED_TUPLE__
            nested = 1
        if typeval.startswith('map<'):
            if 'tuple<' in typeval:
                nested = 1
        if typeval.startswith('set<'):
            if 'tuple<' in typeval:
                nested = 1
        td += typeval
        td += ' '
        td += name
        i = i + 1
    td += '>'
    
    # For nested tuple types use dict as prefered python object instead of named tuple (problems with named tuples classes)
    if nested: # __NESTED_TUPLE__
        return StreamSchema(td)
    return StreamSchema(td).as_tuple(named=nt.__name__)
    
    
def _from_named_tuple_subclass(nt):
    i = 0
    td = ''
    for name in nt._fields:
        if i:
            td += ', '
        typeval = _spl_from_type(nt._field_types[name])
        td += typeval
        td += ' '
        td += name
        i = i + 1
    return td
 

def _spl_from_type(type_):
    _init_type_mappings()
    if type_ in _PYTYPE_TO_SPL:
        return _PYTYPE_TO_SPL[type_]

    # See https://bugs.python.org/issue34568
    # isinstance,issubclass no longer work in Python 3.7
    if hasattr(type_, '__origin__') and hasattr(type_, '__args__'):
        if len(type_.__args__) == 1:
            et = type_.__args__[0]
            if typing.List[et] == type_:
                return 'list<' + _spl_from_type(et) + '>'
            if typing.Set[et] == type_:
                return 'set<' + _spl_from_type(et) + '>'
        elif len(type_.__args__) == 2:
            kt = type_.__args__[0]
            vt = type_.__args__[1]
            if typing.Mapping[kt, vt] == type_:
                return 'map<' + _spl_from_type(kt) + ', ' + _spl_from_type(vt) + '>'
    if hasattr(type_, '__args__') and len(type_.__args__) == 2:
        if type(None) in type_.__args__:
            et = type_.__args__[0] if type_.__args__[1] is type(None) else type_.__args__[1]
            if typing.Optional[et] == type_:
                return 'optional<' + _spl_from_type(et) + '>'
    if _is_namedtuple(type_):
        # special handling for nested tuple
        return 'tuple<' + _from_named_tuple_subclass(type_) + '>'
    else:
        raise ValueError("Unsupported type: " + str(type_))

def _type_from_spl(type_):
    _init_type_mappings()
    if type_ in _SPLTYPE_TO_PY:
        return _SPLTYPE_TO_PY[type_]
    if isinstance(type_, tuple):
        if type_[0] == 'list':
            return typing.List[_type_from_spl(type_[1])]
        if type_[0] == 'set':
            return typing.Set[_type_from_spl(type_[1])]
        if type_[0] == 'map':
            return typing.Mapping[_type_from_spl(type_[1][0]), _type_from_spl(type_[1][1])]
        if type_[0] == 'optional':
            return typing.Optional[_type_from_spl(type_[1])]
    raise ValueError("Unsupported type: " + type_)

_PYTYPE_TO_SPL = {}
_SPLTYPE_TO_PY = {}
def _init_type_mappings():
    global _PYTYPE_TO_SPL
    if not _PYTYPE_TO_SPL:
        import streamsx.spl.types
        _PYTYPE_TO_SPL = {
            str:'rstring', bool:'boolean', int:'int64', float:'float64',
            complex:'complex64', decimal.Decimal:'decimal128',
            streamsx.spl.types.Timestamp:'timestamp',
            datetime.datetime:'timestamp',
            bytes:'blob' }

        global _SPLTYPE_TO_PY
        _SPLTYPE_TO_PY = {
            'rstring': str, 'boolean':bool,
            'int8':int, 'int16':int, 'int32':int, 'int64':int,
            'uint8':int, 'uint16':int, 'uint32':int, 'uint64':int,
            'float32':float, 'float64':float,
            'complex32':complex, 'complex64':complex,
            'decimal32':decimal.Decimal,
            'decimal64':decimal.Decimal,
            'decimal128':decimal.Decimal,
            'timestamp':streamsx.spl.types.Timestamp,
            'blob': bytes}
