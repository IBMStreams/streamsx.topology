# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015

from streamsx.topology.context.ToolkitStreamsContext import ToolkitStreamsContext
from streamsx.topology.context.StandaloneBundleStreamsContext import StandaloneBundleStreamsContext
from streamsx.topology.context.StandaloneStreamsContext import StandaloneStreamsContext
from streamsx.topology.context.DistributedStreamsContext import DistributedStreamsContext
from streamsx.topology.context.DistributedBundleStreamsContext import DistributedBundleStreamsContext

from streamsx.topology.context.Types import Types

class StreamsContextFactory:
    """A factory for getting different submission contexts"""

    @staticmethod
    def get_streams_context(type):
        if type == Types.TOOLKIT:
            return ToolkitStreamsContext()
        elif type == Types.STANDALONE_BUNDLE:
            return StandaloneBundleStreamsContext()
        elif type == Types.STANDALONE:
            return StandaloneStreamsContext()
        elif type == Types.DISTRIBUTED_BUNDLE:
            return DistributedBundleStreamsContext()
        elif type == Types.DISTRIBUTED:
            return DistributedStreamsContext()
        else:
            raise Exception("\"" + type + "\" is not a known submission context")
