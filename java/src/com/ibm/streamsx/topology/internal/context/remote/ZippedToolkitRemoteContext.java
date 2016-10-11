package com.ibm.streamsx.topology.internal.context.remote;

import com.ibm.streamsx.topology.context.remote.RemoteContext.Type;

public class ZippedToolkitRemoteContext extends ToolkitRemoteContext {
    @Override
    public Type getType() {
        return Type.ZIPPED_TOOLKIT;
    } 
}
