/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016 
 */
package com.ibm.streamsx.topology.internal.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.JOB_CONFIG;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.JOB_CONFIG_OVERLAYS;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.OPERATION_CONFIG;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.OVERRIDE_RESOURCE_LOAD_PROTECTION;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.gson;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
    
    /**
     * Get a JobConfig from the JOB_CONFIG_OVERLAYS
     * object in the deploy object. The deploy object
     * always uses JOB_CONFIG_OVERLAYS style even if
     * the Streams version doesn't support overlays (pre 4.2).
     * In that case individual items are taken from the 
     * JobConfig created from the overlay.
     */
    public static JobConfig fromFullOverlay(JsonObject deploy) {
        
        JobConfig jobConfig;
        JsonArray jcos = array(deploy, JOB_CONFIG_OVERLAYS);
        if (jcos == null || jcos.size() == 0) {
            jobConfig =  new JobConfig();
        // assume a single config, only one supported in 4.2
        } else {

            JsonObject jco = jcos.get(0).getAsJsonObject();

            if (jco.has(JOB_CONFIG))
                jobConfig = gson().fromJson(object(jco, JOB_CONFIG), JobConfig.class);
            else
                jobConfig = new JobConfig();
            
            if (jco.has(OPERATION_CONFIG)) {
                JsonObject operationConfig = object(jco, OPERATION_CONFIG);
                if (operationConfig != null) {
                    if (operationConfig.has(OVERRIDE_RESOURCE_LOAD_PROTECTION)) {
                        jobConfig.setOverrideResourceLoadProtection(
                                jboolean(operationConfig, OVERRIDE_RESOURCE_LOAD_PROTECTION));
                    }
                }
            }
        }
        
        return jobConfig;
    }
        
    /**
     * Add the job config overlays to the
     * top-level submission deployment object.
     */
    public JsonObject fullOverlayAsJSON(JsonObject deploy) {
                     
        JsonObject overlay = new JsonObject();
        
        // JobConfig
        JsonObject jsonJobConfig = gson().toJsonTree(jobConfig).getAsJsonObject();
        if (!jsonJobConfig.entrySet().isEmpty()) {
             overlay.add(JOB_CONFIG, jsonJobConfig);
        }
        
        if (jobConfig.getOverrideResourceLoadProtection() != null) {
            JsonObject operationConfig = new JsonObject();
            operationConfig.addProperty(OVERRIDE_RESOURCE_LOAD_PROTECTION,
                    jobConfig.getOverrideResourceLoadProtection());
            overlay.add(OPERATION_CONFIG, operationConfig);
        }
        
        // Create the top-level structure.
        JsonArray jcos = new JsonArray();
        jcos.add(overlay);
        deploy.add(JOB_CONFIG_OVERLAYS, jcos);
        
        return deploy;
    }
}
