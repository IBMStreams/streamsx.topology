/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    @Expose
    private String applicationLogTrace;
    
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
        long end = start + unit.toMillis(timeout);
        long sleepTime = 200;
        Set<String> healthyPes = new HashSet<>();
        int healthyPeCount = 0;

        while (!"healthy".equals(getHealth())) {
            
            long now = System.currentTimeMillis();
            
            if (now > end)
                throw new TimeoutException();
                                    
            for (ProcessingElement pe : getPes()) {
                String id = pe.getId();
                if ("healthy".equals(pe.getHealth())) {
                    if (!healthyPes.contains(id))
                        healthyPes.add(id);
                }
            }
            if (healthyPes.size() > healthyPeCount) {
                // making progress - delay the timeout.
                now = System.currentTimeMillis();
                if (now > end)
                    end = now + (2 * sleepTime);
                else
                    end += (2 * sleepTime);

                healthyPeCount = healthyPes.size();
            }
            
            // backoff if it seems like it is unlikely to start
            if ((now - start) > 5000) {
                sleepTime = 1000;
            }
            
            Thread.sleep(sleepTime);
            refresh();
        }
    }
    
    /**
     * Retrieves the application log and trace files of the job
     * and saves them as a compressed tar file.
     * <BR>
     * The resulting file name is {@code job_<id>_<timestamp>.tar.gz} where {@code id} is the
     * job identifier and {@code timestamp} is the number of seconds since the Unix epoch,
     * for example {@code job_355_1511995995.tar.gz}.
     * 
     * @param directory a valid directory in which to save the archive.
     * Defaults to the current directory.

     * @return  File obhject representing the created tar file, or {@code null} if retrieving a job's
     * logs is not supported in the version of IBM Streams to which the job is submitted.
     * @throws IOException
     * 
     * @since 1.11
     */
    public File retrieveLogTrace(File directory) throws IOException {
    	if (this.applicationLogTrace == null)
    		return null;
    	
    	File fn;
    	String lfn = "job_" + this.id + "_" + (System.currentTimeMillis()/1000L) + ".tar.gz";
    	if (directory == null) {
    		fn = new File(lfn);
    	} else {
    		fn = new File(directory, lfn);
    	}
    	
    	StreamsRestUtils.getFile(connection().getExecutor(), connection().getAuthorization(), applicationLogTrace, fn);
    	
    	return fn;
    }

    private static class JobArray  extends ElementArray<Job> {
        @Expose
        private ArrayList<Job> jobs;
        @Override
        List<Job> elements() { return jobs; }
    }

}
