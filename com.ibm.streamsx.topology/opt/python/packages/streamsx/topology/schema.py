import enum

class StreamSchema(object) :
    """SPL Schema"""

    def __init__(self, schema):
        self.__schema=schema

    def schema(self):
        return self.__schema;

# XML = StreamSchema("tuple<xml document>")

@enum.unique
class CommonSchema(enum.Enum):
    Python = StreamSchema("tuple<blob __spl_po>")
    Json = StreamSchema("tuple<rstring jsonString>")
    String = StreamSchema("tuple<rstring string>")
    Binary = StreamSchema("tuple<blob binary>")

    def schema(self):
        return self.value.schema();

    def subscribeOp(self):
        if (self == CommonSchema.String):
            return "com.ibm.streamsx.topology.topic::SubscribeString"
