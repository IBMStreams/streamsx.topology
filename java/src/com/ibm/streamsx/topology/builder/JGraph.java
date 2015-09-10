/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.builder;

import com.ibm.json.java.JSONObject;

public class JGraph {
    
    /**
     * Create the config object if it has not already been created.
     * @return A new or existing config object.
     */
    public static JSONObject createConfig(JSONObject graph) {
        JSONObject config = (JSONObject) graph.get("config");
        if (config == null)
            config = new JSONObject();
        return config;

    }

}
