/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.context.RemoteContextForwarderStreamsContext;
import com.ibm.streamsx.topology.internal.context.streamsrest.DistributedStreamsRestContext;

/**
 * Context that submits the SPL to the Stream build service
 * for a remote build.
 * 
 * Delegates to DistributedStreamsRestContext.
 * 
 */
public class RemoteDistributedStreamsContext extends RemoteContextForwarderStreamsContext<BuildService> {
        
    public RemoteDistributedStreamsContext() {
        super(new DistributedStreamsRestContext());
    }

    @Override
    public com.ibm.streamsx.topology.context.StreamsContext.Type getType() {
        return StreamsContext.Type.DISTRIBUTED;
    }
}
