/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.annotations.Expose;

/**
 * Access to information about a resource that is allocated to a Streams instance.
 * 
 * @since 1.9
 */
public class ResourceAllocation extends Element {

    @Expose
    private String resourceType;
    @Expose
    private String restid;
    @Expose
    private String resource;
    @Expose
    private boolean applicationResource;
    @Expose
    private String schedulerStatus;
    @Expose
    private String status;
    
    @Expose
    private String pes;
    @Expose
    private String jobs;
    @Expose
    private String instance;
    private Instance instance_;

    /**
     * Identifies the REST resource type
     * 
     * @return "resourceAllocation"
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Indicates whether the resource is an application resource.
     * 
     * Application resources are used to run streams processing applications.
     * @return {@code true} if this is an application resource otherwise {@code false}.
     */
    public boolean isApplicationResource() {
        return applicationResource;
    }

    /**
     * Status of the resource.
     * 
     * Some possible values for this property include
     * {@code failed}, {@code partiallyFailed}, {@code partiallyRunning},
     * {@code quiesced}, {@code quiescing}, {@code running},
     * {@code restarting}, {@code resuming}, {@code starting}, 
     * {@code stopped}, and {@code unknown}.
     * 
     * @return Status of the resource.
     */
    public String getSchedulerStatus() {
        return schedulerStatus;
    }

    /**
     * Scheduler status of the resource.
     * When a resource is schedulable
     * it is available for running streams processing applications.
     * 
     * @return Status of the resource.
     * 
     * @see  <a href="https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.1/com.ibm.streams.admin.doc/doc/resource-status-values.html">
     * Resource status values - Table 2. Schedulable states</a>
     */
    public String getStatus() {
        return status;
    }
    
    /**
     * Obtain the {@code Resource} object for detailed information
     * on the resource.
     * @return Resource.
     * @throws IOException Exception communicating with Streams instance.
     */
    public Resource getResource() throws IOException {
        return create(connection(), resource, Resource.class);
    }
    
    /**
     * Obtain the {@code Instance} this resource is allocated to.
     * @return Instance this resource is allocated to.
     * @throws IOException Exception communicating with Streams instance.
     */
    public Instance getInstance() throws IOException {
        if (instance_ == null)
            instance_ = create(connection(), instance, Instance.class);
        return instance_;
    }
    
    /**
     * Gets a list of jobs running on this resource in this instance.
     * 
     * @return List of {@link Job IBM Streams Jobs}
     * @throws IOException Exception communicating with Streams instance.
     */
    public List<Job> getJobs() throws IOException {
        if (isApplicationResource())
            return Job.createJobList(getInstance(), jobs);
        return Collections.emptyList();
    }
    
    /**
     * Gets a list of processing elements running on this resource in this instance.
     * 
     * @return List of {@link ProcessingElement Processing Elements}
     * @throws IOException Exception communicating with Streams instance.
     */
    public List<ProcessingElement> getPes() throws IOException {
        if (isApplicationResource())
            return ProcessingElement.createPEList(connection(), pes);
        return Collections.emptyList();
    }
    
    static final List<ResourceAllocation> createResourceAllocationList(
            AbstractStreamsConnection sc, String uri) throws IOException {        
        return createList(sc, uri, ResourceAllocationsArray.class);
    }
    
    private static class ResourceAllocationsArray extends ElementArray<ResourceAllocation> {
        @Expose
        private ArrayList<ResourceAllocation> resourceAllocations;
        @Override
        List<ResourceAllocation> elements() { return resourceAllocations; }
    }
}
