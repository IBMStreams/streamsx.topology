package com.ibm.streamsx.topology.internal.context.remote;

public class ZippedToolkitRemoteContext extends ToolkitRemoteContext {
    @Override
    public Type getType() {
        return Type.ZIPPED_TOOLKIT;
    } 
}
