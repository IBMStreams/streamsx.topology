/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import java.io.IOException;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities;

public abstract class BJSONObject {

    private final JSONObject json = new JSONObject();
    private final JsonObject _json = new JsonObject();

    /**
     * Provides direct access to the JSON object, which may not be complete.
     */
    public JSONObject json() {
        return json;
    }
    public final JsonObject _json() {
        return _json;
    }

    public JSONObject complete() {
        
        try {
            JSONObject g = JSON4JUtilities.json4j(_json());
            for (Object k : g.keySet()) {
                json().put(k, g.get(k));
                
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new RuntimeException(e);
        }
       
        
        return json();
    }
}
