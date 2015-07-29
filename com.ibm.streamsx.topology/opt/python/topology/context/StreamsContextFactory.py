from topology.context.ToolkitStreamsContext import ToolkitStreamsContext
from topology.context.StandaloneBundleStreamsContext import StandaloneBundleStreamsContext
from topology.context.StandaloneStreamsContext import StandaloneStreamsContext
from topology.context.DistributedStreamsContext import DistributedStreamsContext
from topology.context.DistributedBundleStreamsContext import DistributedBundleStreamsContext

from topology.context.Types import Types

__author__ = 'wcmarsha'


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