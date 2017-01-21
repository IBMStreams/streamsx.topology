/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.topology.context;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;

class StreamsContextWrapper<T> implements StreamsContext<T> {
    
    static <C> StreamsContext<C> wrap(StreamsContext<C> context) {
       return new StreamsContextWrapper<>(context);
    }
    
    private final StreamsContext<T> context;
    private StreamsContextWrapper(StreamsContext<T> context) {
        this.context = context;
    }
    
    public StreamsContext.Type getType() {
        return context.getType();
    }
    public boolean isSupported(Topology topology) {
        return context.isSupported(topology);
    }
    public Future<T> submit(Topology topology) throws Exception {
        return context.submit(topology);
    }
    public Future<T> submit(Topology topology, Map<String, Object> config) throws Exception {
        // Copy the context to leave the caller's untouched
        return context.submit(topology, new HashMap<>(config));
    }
    public Future<T> submit(JSONObject submission) throws Exception {
        // Copy the context to leave the caller's untouched
        return context.submit((JSONObject) submission.clone());
    }


}
