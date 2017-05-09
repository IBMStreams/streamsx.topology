/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.List;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * {@Instance}
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

    /**
     * this function is not intended for external consumption
     */
    static final Instance create( final StreamsConnection sc, String gsonInstance ) {
        Instance instance = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                                             .fromJson(gsonInstance, Instance.class);
        instance.setConnection(sc);
        return instance ;
    }

    /**
     * this function is not intended for external consumption
     */
    void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    /**
     * @return List of {@Job}
     * @throws IOException
     */
    public List<Job> getJobs() throws IOException {
        String sReturn = connection.getResponseString(jobs);

        List<Job> lJobs = new JobsArray(connection, sReturn).getJobs();
        return lJobs;
    }

    /**
     * @param jobId
     * @return {@Job}
     * @throws IOException
     */
    public Job getJob(String jobId) throws IOException {
        String sGetJobURI = jobs + "/" + jobId;

        String sReturn = connection.getResponseString(sGetJobURI);
        Job job = Job.create(connection, sReturn);
        return job;
    }

    public String getActiveServices() {
        return activeServices;
    }

    public ActiveVersion getActiveVersion() {
        return activeVersion;
    }

    public String getActiveViews() {
        return activeViews;
    }

    public String getConfiguredViews() {
        return configuredViews;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String getCreationUser() {
        return creationUser;
    }

    public String getDomain() {
        return domain;
    }

    public String getExportedStreams() {
        return exportedStreams;
    }

    public String getHealth() {
        return health;
    }

    public String getHosts() {
        return hosts;
    }

    public String getId() {
        return id;
    }

    public String getImportedStreams() {
        return importedStreams;
    }

    public String getOperatorConnections() {
        return operatorConnections;
    }

    public String getOperators() {
        return operators;
    }

    public String getOwner() {
        return owner;
    }

    public String getPeConnections() {
        return peConnections;
    }

    public String getPes() {
        return pes;
    }

    public String getResourceAllocations() {
        return resourceAllocations;
    }

    public String getResourceType() {
        return resourceType;
    }

    public String getRestid() {
        return restid;
    }

    public String getSelf() {
        return self;
    }

    public long getStartTime() {
        return startTime;
    }

    public String getStartedBy() {
        return startedBy;
    }

    public String getStatus() {
        return status;
    }

    public String getViews() {
        return views;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
