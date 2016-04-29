import enum

class StreamSchema(object) :
    """SPL stream schema"""

    def __init__(self, schema):
        self.__schema=schema.strip()

    def schema(self):
        return self.__schema;

    def spl_json(self):
        _splj = {}
        _splj["type"] = 'spltype'
        _splj["value"] = self.schema()
        return _splj

    # Extend a schema by another
    def extend(self, schema):
        base = self.schema()
        extends = schema.schema()
        new_schema = base[:-1] + ',' + extends[6:]
        return StreamSchema(new_schema)

# XML = StreamSchema("tuple<xml document>")

@enum.unique
class CommonSchema(enum.Enum):
    """Common stream schemas for interoperability"""
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
