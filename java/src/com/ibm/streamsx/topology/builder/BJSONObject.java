/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.google.gson.JsonObject;

public abstract class BJSONObject {

    private final JsonObject _json = new JsonObject();

    /**
     * Provides direct access to the JSON object, which may not be complete.
     */
    public final JsonObject _json() {
        return _json;
    }
    
    public JsonObject _complete() {       
        return _json();
    }
}
