# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017
"""
Schemas for streams.

On a structured stream a tuple is a sequence of attributes,
and an attribute is a named value of a specific type.

The supported types are defined by IBM Streams Streams Processing Language (SPL).
"""
import enum

def _stream_schema(schema):
    if isinstance(schema, StreamSchema):
        return schema
    if isinstance(schema, CommonSchema):
        return schema
    return StreamSchema(str(schema))

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

    Args:
        schema(str): Schema definition. Either a schema definition or the name of an SPL type.
    """
    def __init__(self, schema):
        schema = schema.strip()
        self.__spl_type = not schema.startswith("tuple<")
        self.__schema=schema

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

# XML = StreamSchema("tuple<xml document>")

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
    Stream where each tuple is a Python object.

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

    Python objects are converted to strings using `str(obj)`.
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
