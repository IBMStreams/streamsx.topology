/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

import com.ibm.streamsx.topology.jobconfig.JobConfig;

/**
 * Job properties specific to distributed contexts.
 * <BR>
 * The preferred mechanism is to supply a job configuration
 * object using {@link #CONFIG} rather than the individual values.
 * 
 * @see StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)
 * @see ContextProperties
 * @see StreamsContext.Type#DISTRIBUTED
 * @see StreamsContext.Type#STREAMING_ANALYTICS_SERVICE
 * @see StreamsContext.Type#ANALYTICS_SERVICE
 */
public interface JobProperties {
    
    /**
     * Configuration for a submitted application.
     * This single property ({@value}) contains the full submission
     * time configuration for the application.
     * This property overrides all other {@code JobProperties}
     * such as {@link #NAME} and {@link #GROUP}.
     * <BR>
     * Argument is a {@link JobConfig} object.
     * <p>
     * Setting a job configuration is optional.
     * </p>
     */
    String CONFIG = "jobConfig";
    
    /**
     * Name for a submitted  application.
     * Argument is a String.
     * <p>
     * Specifying a job name is optional.
     * </p>
     */
    String NAME = "job.name";
    
    /**
     * Group name for a submitted application.
     * Argument is a String.
     * <p>
     * Specifying a job group is optional.  
     * When specified an existing Job Group must be supplied.
     * By default a Job is added to the job group "default".
     * </p>
     */
    String GROUP = "job.group";
    
    /**
     * Optional override for a submitted application.
     * Argument is a Boolean.
     * <p>
     * Specifies whether to submit the job regardless of the load settings
     * for the target resources.
     * </p>
     */
    String OVERRIDE_RESOURCE_LOAD_PROTECTION = "job.overrideResourceLoadProtection";
    
    /**
     * Optionally specify whether to preload the job onto all resources in
     * the instance. Valid values are true and false.
     * Preloading the job can improve the performance if PE is relocated to
     * a new resource.
     */
    String PRELOAD_APPLICATION_BUNDLES = "job.preloadApplicationBundles";

    /**
     * Location of the submitted application's data directory.
     * The location, full pathname, must be accessible from the resources
     * where the application is deployed.
     */
    String DATA_DIRECTORY = "job.dataDirectory";
}
