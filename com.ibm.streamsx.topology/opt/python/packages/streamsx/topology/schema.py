# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

import enum

class StreamSchema(object) :
    """SPL stream schema."""

    def __init__(self, schema):
        self.__schema=schema.strip()

    def schema(self):
        return self.__schema;

    def spl_json(self):
        _splj = {}
        _splj["type"] = 'spltype'
        _splj["value"] = self.schema()
        return _splj

    def extend(self, schema):
        """
        Extend a schema by another
        """
        base = self.schema()
        extends = schema.schema()
        new_schema = base[:-1] + ',' + extends[6:]
        return StreamSchema(new_schema)

# XML = StreamSchema("tuple<xml document>")

@enum.unique
class CommonSchema(enum.Enum):
    """
    Common stream schemas for interoperability within Streams applications.
    
    Python - Stream constains Python objects
    Json - Stream contains JSON objects. Streams with schema Json can be published and subscribed between Streams applications implemented in different languages.
    String - Stream contains strings. Streams with schema String can be published and subscribed between Streams applications implemented in different languages.
    Binary - Stream contains binary tuples. NOT YET SUPPORTED IN Python.
    """
    Python = StreamSchema("tuple<blob __spl_po>")
    Json = StreamSchema("tuple<rstring jsonString>")
    String = StreamSchema("tuple<rstring string>")
    Binary = StreamSchema("tuple<blob binary>")

    def schema(self):
        return self.value.schema();

    def spl_json(self):
        return self.value.spl_json()

    def extend(self, schema):
        return self.value.extend(schema)
