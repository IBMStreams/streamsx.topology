/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.math.BigInteger;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.BuildRemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

/**
 * Context that submits the SPL to the a remote context
 * for a remote build.
 */
public abstract class RemoteContextForwarderStreamsContext<T> extends JSONStreamsContext<BigInteger> {
    
    private final BuildRemoteContext<T> remoteContext;
    
    public RemoteContextForwarderStreamsContext(BuildRemoteContext<T> remoteContext) {
        this.remoteContext = remoteContext;
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
