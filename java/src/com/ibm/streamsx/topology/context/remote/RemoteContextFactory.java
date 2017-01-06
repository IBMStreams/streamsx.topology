package com.ibm.streamsx.topology.context.remote;

import com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.ZippedToolkitRemoteContext;

public class RemoteContextFactory {
    
    public static RemoteContext<?> getRemoteContext(String type) {
        return getRemoteContext(RemoteContext.Type.valueOf(type));
    }

    public static RemoteContext<?> getRemoteContext(RemoteContext.Type type) {
        switch (type) {
        case TOOLKIT:
            return new ToolkitRemoteContext();
        case BUILD_ARCHIVE:
            return new ZippedToolkitRemoteContext();
	case ANALYTICS_SERVICE:
	    return new RemoteBuildAndSubmitRemoteContext();
        default:
            throw new IllegalArgumentException("Unknown type:" + type);
        }
    }
}
