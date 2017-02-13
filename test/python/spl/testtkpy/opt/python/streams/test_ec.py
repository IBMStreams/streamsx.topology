# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

# Import the SPL decorators
from streamsx.spl import spl
import streamsx.ec as ec

#------------------------------------------------------------------
# Test Execution Context (streamsx.ex) functions
#------------------------------------------------------------------

def splNamespace():
    return "com.ibm.streamsx.topology.pytest.pyec"

@spl.filter()
class PyTestOperatorContext:
    def __init__(self, job_id, pe_id, channel, local_channel, max_channels, local_max_channels):
        self.last = None
        self.job_id = job_id
        self.pe_id = pe_id
        self.channel = channel
        self.local_channel = local_channel
        self.max_channels = max_channels
        self.local_max_channels = local_max_channels
        if not self.check():
             raise AssertionError("PyTestOperatorContext")

    def same(self, expect, got):
        if expect != got:
            print("Expected", expect, "Got", got, flush=True)
            return False
        return True

    def check(self):
        ok = ec._supported
        ok = ok and self.same(self.job_id, ec.job_id())
        ok = ok and self.same(self.pe_id, ec.pe_id())
        ok = ok and self.same(self.channel, ec.channel(self))
        ok = ok and self.same(self.local_channel, ec.local_channel(self))
        ok = ok and self.same(self.max_channels, ec.max_channels(self))
        ok = ok and self.same(self.local_max_channels, ec.local_max_channel(self))
        return ok
            
    def __call__(self, *tuple):
        return self.check()
    

