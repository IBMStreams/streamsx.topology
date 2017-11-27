/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.service;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.math.BigInteger;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.context.JSONStreamsContext;
import com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

/**
 * Context that submits the SPL to the Streaming Analytics service
 * for a remote build.
 * 
 * This delegates to the true remote context: RemoteBuildAndSubmitRemoteContext.
 */
public class RemoteStreamingAnalyticsServiceStreamsContext extends JSONStreamsContext<BigInteger> {
    
    private final RemoteBuildAndSubmitRemoteContext remoteContext;
    
    public RemoteStreamingAnalyticsServiceStreamsContext() {
        remoteContext = new RemoteBuildAndSubmitRemoteContext();
    }

    @Override
    public com.ibm.streamsx.topology.context.StreamsContext.Type getType() {
        return StreamsContext.Type.STREAMING_ANALYTICS_SERVICE;
    }

    @Override
    protected Future<BigInteger> action(com.ibm.streamsx.topology.internal.context.JSONStreamsContext.AppEntity entity) throws Exception {
        remoteContext.submit(entity.submission).get();

        // TODO: Be nice if we had a way to reuse the StreamingAnalyticsService
        // that remoteContext used, but this will not make any REST calls so
        // while there is some duplication it should be relatively cheap.
        JsonObject deploy = deploy(entity.submission);
        JsonObject vcapServices = VcapServices.getVCAPServices(deploy.get(VCAP_SERVICES));

        final StreamingAnalyticsService sas = StreamingAnalyticsService.of(vcapServices,
                jstring(deploy, SERVICE_NAME));

        String jobId = sas.getJobId(entity.submission);
        if (jobId != null)
            return new CompletedFuture<>(new BigInteger(jobId));

        throw new IllegalStateException("Job submission failed!");
    }
}
