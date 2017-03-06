/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.util.Collections;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;

abstract class StreamsContextImpl<T> implements StreamsContext<T> {

    @Override
    public final Future<T> submit(Topology topology) throws Exception {
        return submit(topology, Collections.emptyMap());
    }
    
    /**
     * Default implementation.
     * @return true; the context supports the topology
     */
    @Override
    public boolean isSupported(Topology topology) {
        return true;
    }
    
    @Override
    public Future<T> submit(JSONObject submission) throws Exception {
    	throw new UnsupportedOperationException();
    }
}
