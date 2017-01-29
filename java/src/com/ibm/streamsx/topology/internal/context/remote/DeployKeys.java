/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import com.google.gson.JsonObject;

/**
 * Keys in the JSON deploy object for job submission.
 */
public interface DeployKeys {
    
    /**
     * Key for deploy information in top-level submission object.
     */
    String DEPLOY = "deploy";
    
    /**
     * Get deploy object from submission.
     */
    static JsonObject deploy(JsonObject submission) {
        return object(submission, DEPLOY);
    }
    
    /**
     * Python information.
     * A JSON object with:
     * "prefix": sys.exec_prefix
     * "version": sys.version
     */
    String PYTHON = "python";
    
    /**
     * Streams 4.2 job config overlays. Expect value
     * is an array of job config overlays, though
     * only a single one is supported.
     */
    String JOB_CONFIG_OVERLAYS = "jobConfigOverlays";
}
