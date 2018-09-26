/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

/**
 * 
 * An object describing an IBM Streams Instance
 * 
 */
public class Instance extends Element {
    
    @Expose
    private String activeServices;
    @Expose
    private ActiveVersion activeVersion;
    @Expose
    private String activeViews;
    @Expose
    private String configuredViews;
    @Expose
    private long creationTime;
    @Expose
    private String creationUser;
    @Expose String domain;
    @Expose
    private String exportedStreams;
    @Expose
    private String health;
    @Expose
    private String hosts;
    @Expose
    private String id;
    @Expose
    private String importedStreams;
    @Expose
    private String jobs;
    @Expose
    private String operatorConnections;
    @Expose
    private String operators;
    @Expose
    private String owner;
    @Expose
    private String peConnections;
    @Expose
    private String pes;
    @Expose
    private String resourceAllocations;
    @Expose
    private String resourceType;
    @Expose
    private String restid;
    @Expose
    private long startTime;
    @Expose
    private String startedBy;
    @Expose
    private String status;
    @Expose
    private String views;

    final static List<Instance> createInstanceList(AbstractStreamsConnection sc, String uri)
       throws IOException {        
        return createList(sc, uri, InstancesArray.class);
    }

    /**
     * Gets a list of {@link Job jobs} that this instance knows about
     * 
     * @return List of {@link Job IBM Streams Jobs}
     * @throws IOException
     */
    public List<Job> getJobs() throws IOException {
        return Job.createJobList(this, jobs);
    }
    
    /**
     * Gets a list of {@link ProcessingElement processing elements} for this instance.
     * 
     * @return List of {@link ProcessingElement Processing Elements}
     * @throws IOException
     * 
     * @since 1.9
     */
    public List<ProcessingElement> getPes() throws IOException {
        return ProcessingElement.createPEList(connection(), pes);
    }
    
    /**
     * Gets a list of {@link ResourceAllocation resource allocations} for this instance.
     * 
     * @return List of {@link ResourceAllocation resource allocations}
     * @throws IOException
     * 
     * @since 1.9
     */
    public List<ResourceAllocation> getResourceAllocations() throws IOException {
        return ResourceAllocation.createResourceAllocationList(connection(), resourceAllocations);
    }

    /**
     * Gets the {@link Job} for a given jobId in this instance
     * 
     * @param jobId
     *            String identifying the job
     * @return a single {@link Job}
     * @throws IOException
     */
    public Job getJob(String jobId) throws IOException {
        requireNonNull(jobId);
        
        String sGetJobURI = jobs + "/" + jobId;

        String sReturn = connection().getResponseString(sGetJobURI);
        Job job = Job.create(this, sReturn);
        return job;
    }

    /**
     * Gets information about the IBM Streams Installation that was used to
     * start this instance
     * 
     * @return {@link ActiveVersion}
     */
    public ActiveVersion getActiveVersion() {
        return activeVersion;
    }

    /**
     * Gets the time in milliseconds when this instance was created
     * 
     * @return the epoch time in milliseconds when the instance was created as a
     *         long
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Gets the user ID that created this instance
     * 
     * @return the creation user ID
     */
    public String getCreationUser() {
        return creationUser;
    }

    /**
     * Gets the summarized status of jobs in this instance
     *
     * @return the summarized status that contains one of the following values:
     *         <ul>
     *         <li>healthy</li>
     *         <li>partiallyHealthy</li>
     *         <li>partiallyUnhealthy</li>
     *         <li>unhealthy</li>
     *         <li>unknown</li>
     *         </ul>
     * 
     */
    public String getHealth() {
        return health;
    }

    /**
     * Gets the IBM Streams unique identifier for this instance
     * 
     * @return the IBM Streams unique idenitifer
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the user ID that represents the owner of this instance
     * 
     * @return the owner user ID
     */
    public String getOwner() {
        return owner;
    }

    /**
     * Identifies the REST resource type
     *
     * @return "instance"
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Gets the time in milliseconds when the instance was started.
     * 
     * @return the epoch time in milliseconds when the instance was started as a
     *         long
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Gets the status of the instance
     *
     * @return the instance status that contains one of the following values:
     *         <ul>
     *         <li>running</li>
     *         <li>failed</li>
     *         <li>stopped</li>
     *         <li>partiallyFailed</li>
     *         <li>partiallyRunning</li>
     *         <li>starting</li>
     *         <li>stopping</li>
     *         <li>unknown</li>
     *         </ul>
     * 
     */
    public String getStatus() {
        return status;
    }
    

    private String appConsoleURL;
    void setApplicationConsoleURL(String baseUrl) throws UnsupportedEncodingException {
        appConsoleURL = baseUrl
               +  "#application/dashboard/Application%20Dashboard?instance="
               + URLEncoder.encode(getId(), "UTF-8");
    }
    /**
     * Streams application console URL.
     * Returns the Streams application console URL with
     * a filter preset to this instance identifier.
     * @return Streams application console URL
     * 
     * @since 1.11
     */
    public String getApplicationConsoleURL() {
        return appConsoleURL;
    }
    
    private Domain _domain;
    /**
     * Get the Streams domain for this instance.
     * 
     * @return Domain for this instance.ull if no domain is associated with an instance.
     * 
     * @throws IOException Error communicating with REST api.
     * 
     * @since 1.8
     */
    public Domain getDomain() throws IOException {
        if (_domain == null) {
            _domain = create(connection(), domain, Domain.class);
        }
        return _domain;
    }
    
    /**
     *  Upload a Streams application bundle (sab) to the instance.
     *  
     *  Uploading a bundle allows job submission from the returned {@link ApplicationBundle}.
     *  
     *  <BR>
     *  Note: When an instance does not support uploading a bundle the returned
     *  {@code ApplicationBundle} represents the local file {@code bundle} tied to this
     *  instance. The returned object  may still be used for job submission.
     *  
     * @param bundle path to a Streams application bundle (sab file) containing
     *     the application to be uploaded
     * @return  Application bundle representing the uploaded bundle.
     * 
     * @throws IOException Error uploading the bundle.
     * 
     * @since 1.11
     */
    public ApplicationBundle uploadBundle(File bundle) throws IOException {
    	return connection().uploadBundle(this, bundle);
    }
    
    /**
     * Submit a Streams bundle to run on the Streaming Analytics Service.
     * <P>
     * The returned {@link Result} instance has:
     * <UL>
     * <LI>{@link Result#getId()} returning the job identifier or {@code null} if
     * a job was not created..</LI>
     * <LI>{@link Result#getElement()} returning a {@link Job} instance for the submitted job or {@code null} if
     * a job was not created.</LI>
     * <LI>{@link Result#getRawResult()} return the raw JSON response.</LI>
     * </UL>
     * </P>
     * @param bundle A streams application bundle
     * @param jco Job configuration overlay in JSON format.
     * @return Result of the job submission.
     * @throws IOException Error communicating with the service.
     * 
     * @since 1.11
     */
    public Result<Job,JsonObject> submitJob(File bundle, JsonObject jco) throws IOException {
    	return uploadBundle(bundle).submitJob(jco);
    }

    /**
     * internal usae to get list of instances
     */
    private static class InstancesArray extends ElementArray<Instance> {
        @Expose
        private ArrayList<Instance> instances;
        
        @Override
        List<Instance> elements() { return instances; }
    }
}
