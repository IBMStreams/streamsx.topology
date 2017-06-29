# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016

# Import the SPL decorators
from streamsx.spl import spl

#------------------------------------------------------------------
# Test passing in SPL types functions
#------------------------------------------------------------------

# Defines the SPL namespace for any functions in this module
# Multiple modules can map to the same namespace
def splNamespace():
    return "com.ibm.streamsx.topology.pytest.pytypes"

@spl.map()
def ToBlob(*s):
    return (s[0].encode('utf-8'),)

@spl.map()
class BlobTest:
    """
    Expect blob tuples, need to verify that after
    the call the previous value cannot be accessed.
    """
    def __init__(self):
        self.last = list()

    def __call__(self, *tuple):
        v = tuple[0]
        hash(v)
        if not isinstance(v, memoryview):
            return ("Expected memory view is" + str(type(v)),)

        if not v.readonly:
            return "Expected readonly memory view",
           
        if self.last:
            for b in self.last:
                print("TRYING LAST")
                try:
                    hash(b)
                    print("GOT_VALUE LAST")
                    return "Expected released memory view",
                except ValueError as ve:
                    print("VALUE_ERRRO", ve)
                    pass
                
        self.last.append(v)
        return str(v, 'utf-8'),

