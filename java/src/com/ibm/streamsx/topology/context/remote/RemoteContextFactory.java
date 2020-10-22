package com.ibm.streamsx.topology.context.remote;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.Sas4BuildContext;
import com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.ZippedToolkitRemoteContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.BuildServiceContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.DistributedCp4dRestContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.DistributedStreamsRestContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.EdgeImageContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.StreamsKeys;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.internal.streams.Util;

public class RemoteContextFactory {
    
    public static RemoteContext<?> getRemoteContext(String type, JsonObject deploy) {
        return getRemoteContext(RemoteContext.Type.valueOf(type), true, deploy);
    }

    public static RemoteContext<?> getRemoteContext(final RemoteContext.Type type, final boolean keepArtifact) {
        return getRemoteContext (type, keepArtifact, null);
    }

    public static RemoteContext<?> getRemoteContext(final RemoteContext.Type type, final boolean keepArtifact, final JsonObject deploy) {
        switch (type) {
        case TOOLKIT:
            return new ToolkitRemoteContext(keepArtifact);
        case BUILD_ARCHIVE:
            return new ZippedToolkitRemoteContext(keepArtifact);
        case ANALYTICS_SERVICE:
        case STREAMING_ANALYTICS_SERVICE:
        	return new RemoteBuildAndSubmitRemoteContext();
        case BUNDLE:
        case EDGE_BUNDLE:
            return new BuildServiceContext();
        case DISTRIBUTED:
            if (deploy == null) {
                return new DistributedStreamsRestContext();
            }
            final String productVersion = StreamsKeys.getProductVersion(deploy);
            if (productVersion == null) {
                return new DistributedStreamsRestContext();
            }
            if (Util.versionAtLeast(productVersion, 5,  5)) {
                return new DistributedCp4dRestContext();
            }
            return new DistributedStreamsRestContext();

        case SAS_BUNDLE:
            return new Sas4BuildContext();
        case EDGE:
            return new EdgeImageContext();
        default:
            throw new IllegalArgumentException(Messages.getString("CONTEXT_UNKNOWN_TYPE", type));
        }
    }
}
