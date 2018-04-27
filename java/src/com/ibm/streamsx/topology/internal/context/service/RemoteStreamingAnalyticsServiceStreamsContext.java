/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.service;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.math.BigInteger;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.JSONStreamsContext;
import com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.messages.Messages;

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

        JsonObject results = object(entity.submission, RemoteContext.SUBMISSION_RESULTS);
        if (results != null) {
            String jobId = jstring(results, SubmissionResultsKeys.JOB_ID);
            if (jobId != null)
                return new CompletedFuture<>(new BigInteger(jobId));
        }
        
        throw new IllegalStateException(Messages.getString("CONTEXT_JOB_SUBMISSION_FAILED"));
    }
}
