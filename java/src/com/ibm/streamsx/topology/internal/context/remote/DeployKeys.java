/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

/**
 * Keys in the JSON deploy objcet for job submission.
 */
public interface DeployKeys {
    
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
