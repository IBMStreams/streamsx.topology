/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.context;

/**
 * Job properties specific to a {@link StreamsContext.Type#DISTRIBUTED}
 * context.
 * @see StreamsContext#submit(com.ibm.streamsx.topology.Topology, java.util.Map)
 * @see ContextProperties
 */
public interface JobProperties {
    
    /**
     * Name for a submitted  application.
     * Argument is a String.
     * <p>
     * Specifying a job name is optional.
     */
    String NAME = "job.name";
    
    /**
     * Group name for a submitted application.
     * Argument is a String.
     * <p>
     * Specifying a job group is optional.  
     * When specified an existing Job Group must be supplied.
     * By default a Job is added to the job group "default".
     */
    String GROUP = "job.group";
    
    /**
     * Optional override for a submitted application.
     * Argument is a Boolean.
     * <p>
     * Specifies whether to submit the job regardless of the load settings
     * for the target resources.
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
