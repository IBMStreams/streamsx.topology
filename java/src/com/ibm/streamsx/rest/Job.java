/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

/**
 * An object describing an IBM Streams Job submitted within a specified instance
 */
public class Job {
    private StreamsConnection connection;

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
    private String self;
    @Expose
    private String startedBy;
    @Expose
    private String status;
    @Expose
    private ArrayList<String> submitParameters;
    @Expose
    private long submitTime;
    @Expose
    private String views;

    /**
     * this function is not intended for external consumption
     */
    static final Job create(StreamsConnection sc, String gsonJobString) {
        Job job = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(gsonJobString, Job.class);
        job.setConnection(sc);
        return job;
    }

    /**
     * this function is not intended for external consumption
     */
    void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    /**
     * Gets a list of {@link Operator operators} for this job
     * 
     * @return List of {@link Operator IBM Streams Operators}
     * @throws IOException
     */
    public List<Operator> getOperators() throws IOException {
        String sReturn = connection.getResponseString(operators);

        List<Operator> oList = new OperatorsArray(connection, sReturn).getOperators();
        return oList;
    }

    /**
     * Cancels this job
     * 
     * @return the result of the cancel method
     *         <ul>
     *         <li>true if this job is cancelled
     *         <li>false if this job still exists
     *         </ul>
     * @throws IOException
     * @throws Exception
     */
    public boolean cancel() throws Exception, IOException {
        return connection.cancelJob(id);
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
     *         <li>healthy
     *         <li>partiallyHealthy
     *         <li>partiallyUnhealthy
     *         <li>unhealthy
     *         <li>unknown
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
        String sReturn = connection.getResponseString(pes);
        List<ProcessingElement> peList = ProcessingElement.getPEList(connection, sReturn);
        return peList;
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
     *         <li>canceling
     *         <li>running
     *         <li>canceled
     *         <li>unknown
     *         </ul>
     */
    public String getStatus() {
        return status;
    }

    /**
     * Gets the list of parameters that were submitted to this job
     * 
     * @return List of parameters 
     */
    public List<String> getSubmitParameters() {
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

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
