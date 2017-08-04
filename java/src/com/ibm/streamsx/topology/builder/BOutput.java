/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;

public abstract class BOutput extends BJSONObject {

    public abstract StreamSchema schema();

    public abstract void connectTo(BInputPort port);
    
    public final JSONObject complete() {
        // ONLY GSON
        return XXXcomplete();
    }
    
    public JSONObject json() {
        throw new IllegalStateException("NO JSON4J!!!!");
    }
}
