package com.ibm.streamsx.topology.internal.context.remote;

import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;

public abstract class RemoteContextImpl<T> implements RemoteContext<T> {
    @Override
    public Future<T> submit(JsonObject submission) throws Exception {
        preSubmit(submission);
        return postSubmit(submission, _submit(submission));
    }
    
    public Future<T> _submit(JsonObject submission) throws Exception {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Pre-submit hook when submitting a Topology.
     */
    void preSubmit(JsonObject submission){
        
    }
    
    /**
     * Post-submit hook when submitting a Topology.
     */
    Future<T> postSubmit(JsonObject submission, Future<T> future) throws Exception{
        RemoteContexts.writeResultsToFile(submission);
        return future;
    }
}
