/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities;

abstract class StreamsContextImpl<T> implements StreamsContext<T> {

    @Override
    public final Future<T> submit(Topology topology) throws Exception {
        return submit(topology, Collections.emptyMap());
    }
    
    /**
     * Force a copy of the config to avoid modifying the passed in config.
     */
    @Override
    public final Future<T> submit(Topology topology, Map<String, Object> config) throws Exception {
        return _submit(topology, new HashMap<>(config));
    }
    
    /**
     * The workhorse for submit, can modify its config so this
     * variant (_submit) should be called by sub-classes instead
     * of (submit) to allow data to be exchanged through config values.
     */
    abstract Future<T> _submit(Topology topology, Map<String, Object> config) throws Exception;

    /**
     * Default implementation.
     * @return true; the context supports the topology
     */
    @Override
    public boolean isSupported(Topology topology) {
        return true;
    }
    
    @Override
    public final Future<T> submit(JSONObject submission) throws Exception {
    	return _submit(JSON4JUtilities.gson(submission));
    }
    
    /**
     * Workhorse for handling a JSON graph. Sub-classes use Gson
     * to allow sharing code between remote (non-install) contexts
     * and product install contexts.
     */
    Future<T> _submit(JsonObject submission) throws Exception {
        throw new UnsupportedOperationException();
    } 
}
