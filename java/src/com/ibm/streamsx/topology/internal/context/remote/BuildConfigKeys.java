/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018 
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import com.google.gson.JsonObject;

public interface BuildConfigKeys {
    
    String ORIGINATOR = com.ibm.streamsx.topology.internal.context.remote.DeployKeys.ORIGINATOR;
    
    String KEEP_ARTIFACTS = "keepArtifacts";
    
    
    static JsonObject determineBuildConfig(JsonObject deploy, JsonObject submission) {
        JsonObject buildConfig = new JsonObject();
        
        JsonObject graph = object(submission, "graph");
        
        String originator = determineOriginator(deploy, graph);
        if (originator != null)
             buildConfig.addProperty(ORIGINATOR, originator);
               
        return buildConfig;
    }
    
    static String determineOriginator(JsonObject deploy, JsonObject graph) {
        String originator = jstring(deploy, ORIGINATOR);
        if (originator != null)
            return originator;
        
        originator = jstring(graph, ORIGINATOR);
        if (originator != null)
            return originator;
        
        return null;
    }
}
