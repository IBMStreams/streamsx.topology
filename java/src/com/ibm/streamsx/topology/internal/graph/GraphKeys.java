/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.graph;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import com.google.gson.JsonObject;

/**
 * Keys in the JSON graph object for job submission.
 * 
 * Keys whose field name starts with CFG are stored in
 * the "config" area of the graph.
 */
public interface GraphKeys {
    
    /**
     * Key for graph in top-level submission object.
     */
    String GRAPH = "graph";
    
    /**
     * Key for graph config in graph object.
     */
    String CONFIG = "config";
    
    /**
     * Get graph object from submission.
     */
    static JsonObject graph(JsonObject submission) {
        return object(submission, GRAPH);
    }
    
    /**
     * Get graph config object from submission.
     */
    static JsonObject graphConfig(JsonObject submission) {
        return objectCreate(submission, GRAPH, CONFIG);
    }
    
    /**
     * Does the graph include isolate virtual markers.
     * Boolean.
     */
    String CFG_HAS_ISOLATE = "hasIsolate";
    
    /**
     * Does the graph include low latency virtual markers/regions.
     * Boolean.
     */
    String CFG_HAS_LOW_LATENCY = "hasLowLatency";
    
    /**
     * Mapping of colocation keys to actual colocate tag.
     * Object containing string to string mapping. 
     */
    String CFG_COLOCATE_TAG_MAPPING = "colocateTagMapping";

    /**
     * Version of Streams the graph is targeted at.
     * (runtime or compile if compile version not set). 
     */
    String CFG_STREAMS_VERSION = "streamsVersion";
    
    /**
     * Version of Streams the graph is targeted at for compile.
     * Optional. 
     */
    String CFG_STREAMS_COMPILE_VERSION = "streamsCompileVersion";
}

