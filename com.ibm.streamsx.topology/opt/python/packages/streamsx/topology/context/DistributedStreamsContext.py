# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

import os
from streamsx.topology.context.DistributedBundleStreamsContext import DistributedBundleStreamsContext

class DistributedStreamsContext(DistributedBundleStreamsContext):
    def __init__(self):
        pass

    def submit(self, topology, config={}):
        super(DistributedStreamsContext, self).submit(topology, config)
        os.system("streamtool submitjob ./" + topology.getName() + "." + topology.getName()+".sab")
