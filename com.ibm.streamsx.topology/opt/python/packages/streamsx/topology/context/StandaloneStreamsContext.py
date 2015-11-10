from streamsx.topology.context.StandaloneBundleStreamsContext import StandaloneBundleStreamsContext
import os

class StandaloneStreamsContext(StandaloneBundleStreamsContext):
    def __init__(self):
        pass

    def submit(self, topology, config={}):
        super(StandaloneStreamsContext, self).submit(topology, config)
        os.system("./" + topology.getName() + "." + topology.getName()+".sab")
