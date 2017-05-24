/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * 
 * An object describing an IBM Streams Instance
 * 
 */
public class Instance {
    private StreamsConnection connection;

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
    @Expose
    private String domain;
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
    private String self;
    @Expose
    private long startTime;
    @Expose
    private String startedBy;
    @Expose
    private String status;
    @Expose
    private String views;

    static final Instance create(final StreamsConnection sc, String gsonInstance) {
        Instance instance = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(gsonInstance,
                Instance.class);
        instance.setConnection(sc);
        return instance;
    }

    private void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    final static List<Instance> getInstanceList(StreamsConnection sc, String instanceGSONList) {
        InstancesArray iArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(instanceGSONList,
                InstancesArray.class);

        for (Instance instance : iArray.instances) {
            instance.setConnection(sc);
        }
        return iArray.instances;
    }

    /**
     * Returns a list of Jobs that this Instance knows about
     * 
     * @return List of {@link Job}
     * @throws IOException
     */
    public List<Job> getJobs() throws IOException {
        String sReturn = connection.getResponseString(jobs);

        List<Job> lJobs = new JobsArray(connection, sReturn).getJobs();
        return lJobs;
    }

    /**
     * Returns the {@link Job} for a given jobId in this instance
     * 
     * @param jobId
     *            String identifying the job
     * @return {@link Job}
     * @throws IOException
     */
    public Job getJob(String jobId) throws IOException {
        String sGetJobURI = jobs + "/" + jobId;

        String sReturn = connection.getResponseString(sGetJobURI);
        Job job = Job.create(connection, sReturn);
        return job;
    }

    /**
     * Provides information about the IBM Streams Installation that was used to
     * start this instance
     * 
     * @return {@link ActiveVersion}
     */
    public ActiveVersion getActiveVersion() {
        return activeVersion;
    }

    /**
     * Provides the time in milliseconds when this instance was created
     * 
     * @return {@link long} representing milliseconds since epoch
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Provides the user ID that created this instance
     * 
     * @return {@link String}
     */
    public String getCreationUser() {
        return creationUser;
    }

    /**
     * Provides the summarized status of jobs in this instance
     * <ul>
     * <li>healthy
     * <li>partiallyHealthy
     * <li>partiallyUnhealthy
     * <li>unhealthy
     * <li>unknown
     * </ul>
     * 
     * @return {@link String}
     */
    public String getHealth() {
        return health;
    }

    /**
     * Provides the IBM Streams unique identifier for this instance
     * 
     * @return {@link String}
     */
    public String getId() {
        return id;
    }

    /**
     * Provides the user ID that represents the Owner of this instance
     * 
     * @return
     */
    public String getOwner() {
        return owner;
    }

    /**
     * Provides the resource type (instance) associated with this object
     * 
     * @return {@link String}
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Provides the time in milliseconds since the instance was started.
     * 
     * @return {@long} representing milliseconds since epoch
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Provides the status of the instance
     * <ul>
     * <li>running
     * <li>failed
     * <li>stopped
     * <li>partiallyFailed
     * <li>partiallyRunning
     * <li>starting
     * <li>stopping
     * <li>unknown
     * </ul>
     * 
     * @return {@link String}
     */
    public String getStatus() {
        return status;
    }

    /**
     * Provides a user friendly printing function for this object
     * 
     * @return {@link String}
     */
    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }

    /**
     * internal usae to get list of instances
     */
    private static class InstancesArray {
        @Expose
        public ArrayList<Instance> instances;
        @Expose
        public String resourceType;
        @Expose
        public int total;
    }
}
