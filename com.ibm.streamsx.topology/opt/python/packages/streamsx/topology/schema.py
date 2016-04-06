import enum

class StreamSchema(object) :
    """SPL stream schema"""

    def __init__(self, schema):
        self.__schema=schema

    def schema(self):
        return self.__schema;

    def spl_json(self):
        _splj = {}
        _splj["type"] = 'spltype'
        _splj["value"] = self.schema()
        return _splj

# XML = StreamSchema("tuple<xml document>")

@enum.unique
class CommonSchema(enum.Enum):
    """Common stream schemas for interoperability"""
    Python = StreamSchema("tuple<blob __spl_po>")
    Json = StreamSchema("tuple<rstring jsonString>")
    String = StreamSchema("tuple<rstring string>")
    Binary = StreamSchema("tuple<blob binary>")
    PythonHash = StreamSchema("tuple<blob __spl_po,int32 __spl_hash>")

    def schema(self):
        return self.value.schema();

    def spl_json(self):
        return self.value.spl_json()
