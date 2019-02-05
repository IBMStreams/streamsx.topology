/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2019  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import java.util.concurrent.Future;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;

public abstract class RemoteContextImpl<T> implements RemoteContext<T> {
    
    public static final Logger PROGRESS =
            Logger.getLogger("com.ibm.streamsx.topology.internal.context.remote.progess");
    static {      
        PROGRESS.setLevel(Level.OFF);
        PROGRESS.setUseParentHandlers(false);
        PROGRESS.addHandler(new ConsoleHandler());
    }
    
    
    @Override
    public final Future<T> submit(JsonObject submission) throws Exception {
        
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
    
    protected void report(String action) {
        PROGRESS.info("!!-streamsx-"+getType()+"-" + action);
    }
}
