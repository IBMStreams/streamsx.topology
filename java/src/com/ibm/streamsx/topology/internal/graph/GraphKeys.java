/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.graph;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import com.google.gson.JsonObject;

/**
 * Keys in the JSON graph object for job submission.
 */
public interface GraphKeys {
    
    /**
     * Key for deploy information in top-level submission object.
     */
    String GRAPH = "graph";
    
    /**
     * Get graph object from submission.
     */
    static JsonObject graph(JsonObject submission) {
        return object(submission, GRAPH);
    }
}

