/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.gson.annotations.Expose;

/**
 * An object describing an IBM Streams Job submitted within a specified instance
 */
public class Job extends Element {

    @Expose
    private String activeViews;
    @Expose
    private String adlFile;
    @Expose
    private String applicationName;
    @Expose
    private String applicationPath;
    @Expose
    private String applicationScope;
    @Expose
    private String applicationVersion;
    @Expose
    private String checkpointPath;
    @Expose
    private String dataPath;
    @Expose
    private String domain;
    @Expose
    private String health;
    @Expose
    private String hosts;
    @Expose
    private String id;
    @Expose
    private String instance;
    @Expose
    private String jobGroup;
    @Expose
    private String name;
    @Expose
    private String operatorConnections;
    @Expose
    private String operators;
    @Expose
    private String outputPath;
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
    private String startedBy;
    @Expose
    private String status;
    @Expose
    private ArrayList<JobSubmitParameters> submitParameters;
    @Expose
    private long submitTime;
    @Expose
    private String views;
    
    private Instance _instance;

    static final Job create(Instance instance, String gsonJobString) {
        Job job = gson.fromJson(gsonJobString, Job.class);
        job.setConnection(instance.connection());
        job._instance = instance;
        return job;
    }

    static final List<Job> createJobList(Instance instance, String uri) throws IOException {
        
        List<Job> jList = createList(instance.connection(), uri, JobArray.class);
        for (Job job : jList) {
            job._instance = instance;
        }
        return jList;
    }

    /**
     * Gets a list of {@link Operator operators} for this job
     * 
     * @return List of {@link Operator IBM Streams Operators}
     * @throws IOException
     */
    public List<Operator> getOperators() throws IOException {
        return Operator.createOperatorList(connection(), operators);
    }

    /**
     * Cancels this job.
     * 
     * @return the result of the cancel method
     *         <ul>
     *         <li>true if this job is cancelled</li>
     *         <li>false if this job still exists</li>
     *         </ul>
     * @throws IOException
     * @throws Exception
     */
    public boolean cancel() throws Exception, IOException {
        return connection().cancelJob(this._instance, id);
    }          

    /**
     * Gets the name of the streams processing application that this job is
     * running
     * 
     * @return the application name
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Gets the health indicator for this job
     * 
     * @return the health indicator containing one of the following values:
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
     * Gets the id of this job
     * 
     * @return the job identifier
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the group this job belongs to
     * 
     * @return the job group
     */
    public String getJobGroup() {
        return jobGroup;
    }

    /**
     * Gets the name of this job
     * 
     * @return the job name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets a list of {@link ProcessingElement processing elements} for this job
     * 
     * @return List of {@link ProcessingElement Processing Elements}
     * @throws IOException
     */
    public List<ProcessingElement> getPes() throws IOException {
        return ProcessingElement.createPEList(connection(), pes);
    }
    
    /**
     * Gets a list of {@link ResourceAllocation resource allocations} for this job.
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
     * Identifies the REST resource type
     * 
     * @return "job"
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Identifies the user ID that started this job
     * 
     * @return the user ID that started this job
     */
    public String getStartedBy() {
        return startedBy;
    }

    /**
     * Describes the status of this job
     * 
     * @return the job status that contains one of the following values:
     *         <ul>
     *         <li>canceling</li>
     *         <li>running</li>
     *         <li>canceled</li>
     *         <li>unknown</li>
     *         </ul>
     */
    public String getStatus() {
        return status;
    }

    /**
     * Gets the list of {@link JobSubmitParameters parameters} that were
     * submitted to this job
     * 
     * @return List of (@link JobSubmitParameters job submission parameters}
     */
    public List<JobSubmitParameters> getSubmitParameters() {
        return submitParameters;
    }

    /**
     * Gets the Epoch time when this job was submitted
     * 
     * @return the epoch time when the job was submitted as a long
     */
    public long getSubmitTime() {
        return submitTime;
    }
    
    /**
     * Wait for this job to become healthy.
     * <BR>
     * When submitted a Streams job has to reach a {@code healthy} state
     * before stream processing is started. During this startup time processing
     * elements are started on resources and then connected. This method
     * allows code to wait for the job to become healthy before using
     * the REST api to monitor its elements.
     * <P>
     * Note that a job may subsequently become unhealthy after this
     * call returns due to failures of processing or resources.
     * </P>
     * 
     * @param timeout Time to wait for the job to become healthy.
     * @param unit Unit for {@code timeout}.
     * 
     * @throws TimeoutException Job did not become healthy in the time allowed.
     * @throws InterruptedException Thread was interrupted.
     * @throws IOException Error communicating with Streams.
     */
    public void waitForHealthy(long timeout, TimeUnit unit) throws TimeoutException, InterruptedException, IOException {
        final long start =  System.currentTimeMillis();
        final long end = start + unit.toMillis(timeout);
        long sleepTime = 200;
        while (!"healthy".equals(getHealth())) {
            
            long now = System.currentTimeMillis();
            
            if (now > end)
                throw new TimeoutException();
            
            // backoff if it seems like it is unlikely to start
            if ((now - start) > 5000) {
                sleepTime = 1000;
            }
            
            Thread.sleep(sleepTime);
            refresh();
        }
    }

    private static class JobArray  extends ElementArray<Job> {
        @Expose
        private ArrayList<Job> jobs;
        @Override
        List<Job> elements() { return jobs; }
    }

}
