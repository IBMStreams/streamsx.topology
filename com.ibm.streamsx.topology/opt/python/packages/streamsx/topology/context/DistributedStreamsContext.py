import os
from topology.context.DistributedBundleStreamsContext import DistributedBundleStreamsContext

__author__ = 'wcmarsha'

class DistributedStreamsContext(DistributedBundleStreamsContext):
    def __init__(self):
        pass

    def submit(self, topology, config={}):
        super(DistributedStreamsContext, self).submit(topology, config)
        os.system("streamtool submitjob ./" + topology.getName() + "." + topology.getName()+".sab")