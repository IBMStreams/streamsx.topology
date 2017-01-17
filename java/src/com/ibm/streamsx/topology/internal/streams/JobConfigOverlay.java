/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016 
 */
package com.ibm.streamsx.topology.internal.streams;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.jobconfig.JobConfig;

/**
 * Utility code to generate a Job Config Overlay,
 * supported in IBM Streams 4.2 & later.
 *
 */
public class JobConfigOverlay {
        
    private final JobConfig jobConfig;
    
    public JobConfigOverlay(JobConfig jobConfig) {  
        this.jobConfig = jobConfig;
    }
    
    
    public String fullOverlay() {
        Gson gson = new Gson();
             
        JsonObject overlay = new JsonObject();
        
        // JobConfig
        JsonObject jsonJobConfig = gson.toJsonTree(jobConfig).getAsJsonObject();
        if (!jsonJobConfig.entrySet().isEmpty()) {             
             overlay.add("jobConfig", jsonJobConfig);
        }
        
        // DeploymentConfig
        JsonObject deploy = new JsonObject();
        deploy.addProperty("fusionScheme", "legacy");
        overlay.add("deploymentConfig", deploy);
        
        // Create the top-level structure.
        JsonObject fullJco = new JsonObject();
        JsonArray jcos = new JsonArray();
        jcos.add(overlay);
        fullJco.add(DeployKeys.JOB_CONFIG_OVERLAYS, jcos);
               
        return gson.toJson(fullJco);
    }
}
