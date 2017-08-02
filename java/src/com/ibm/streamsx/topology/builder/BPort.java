/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;

interface BPort {
    
    JSONObject json();
    
    default void addPortInfo(int index, String name, StreamSchema schema) {
        json().put("name", name);
        json().put("type", schema.getLanguageType());
        json().put("index", index);
        
        json().put("connections", new JSONArray());
    }
    
    default String name() {
        return json().get("name").toString();
    }
    default int index() {
        return ((Number) (json().get("index"))).intValue();
    }
    
    default void connect(BPort other) {
        assert !((JSONArray) (json().get("connections"))).contains(other.name());
        ((JSONArray) (json().get("connections"))).add(other.name());
    }
}
