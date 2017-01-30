/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016 
 */
package com.ibm.streamsx.topology.internal.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.JOB_CONFIG_OVERLAYS;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.jobconfig.JobConfig;

/**
 * Utility code to generate a Job Config Overlay,
 * supported in IBM Streams 4.2 & later.
 *
 */
public class JobConfigOverlay {
        
    private final JobConfig jobConfig;
    private final Gson gson = new Gson();
    
    public JobConfigOverlay(JobConfig jobConfig) {  
        this.jobConfig = jobConfig;
        
    }
    
    // TODO
    public static JobConfig fromFullOverlay(JsonObject deploy) {
        Gson gson = new Gson();
        return gson.fromJson(object(deploy, JOB_CONFIG_OVERLAYS), JobConfig.class);
    }
        
    /**
     * Add the job config overlays to the
     * top-level submission deployment object.
     */
    public JsonObject fullOverlayAsJSON(JsonObject deploy) {
                     
        JsonObject overlay = new JsonObject();
        
        // JobConfig
        JsonObject jsonJobConfig = gson.toJsonTree(jobConfig).getAsJsonObject();
        if (!jsonJobConfig.entrySet().isEmpty()) {             
             overlay.add("jobConfig", jsonJobConfig);
        }
        
        // DeploymentConfig
        JsonObject deploymentConfig = new JsonObject();
        deploymentConfig.addProperty("fusionScheme", "legacy");
        overlay.add("deploymentConfig", deploymentConfig);
        
        // Create the top-level structure.
        JsonArray jcos = new JsonArray();
        jcos.add(overlay);
        deploy.add(JOB_CONFIG_OVERLAYS, jcos);
        
        return deploy;
    }
    
    public String fullOverlay() {
        return gson.toJson(fullOverlayAsJSON(new JsonObject()));
    }
}
