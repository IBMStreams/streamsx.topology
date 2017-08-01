/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.StreamSchema;

class BUtils {
    
    static void addPortInfo(JSONObject json, int index, String name, StreamSchema schema) {
        json.put("name", name);
        json.put("type", schema.getLanguageType());
        json.put("index", index);
        
        json.put("connections", new JSONArray());
    }
}
