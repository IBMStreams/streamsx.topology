/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.service;

import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.context.RemoteContextForwarderStreamsContext;
import com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext;

/**
 * Context that submits the SPL to the Streaming Analytics service
 * for a remote build.
 * 
 * This delegates to the true remote context: RemoteBuildAndSubmitRemoteContext.
 */
public class RemoteStreamingAnalyticsServiceStreamsContext extends RemoteContextForwarderStreamsContext<StreamingAnalyticsService> {
        
    public RemoteStreamingAnalyticsServiceStreamsContext() {
        super(new RemoteBuildAndSubmitRemoteContext());
    }

    @Override
    public com.ibm.streamsx.topology.context.StreamsContext.Type getType() {
        return StreamsContext.Type.STREAMING_ANALYTICS_SERVICE;
    }
}
