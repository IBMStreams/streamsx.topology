/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONObject;

public class BInput extends BJSONObject {

    private final GraphBuilder builder;

    protected BInput(GraphBuilder builder) {
        this.builder = builder;
    }

    public GraphBuilder builder() {
        return builder;
    }
    
    public final JSONObject complete() {
        // ONLY GSON
        return XXXcomplete();
    }
    public JSONObject json() {
        throw new IllegalStateException("NO JSON4J!!!!");
    }
}
