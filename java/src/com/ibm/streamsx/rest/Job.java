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
        Job job = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                                   .fromJson(gsonJobString, Job.class);
        job.setConnection(sc);
        return job ;
    }

    /**
      * this function is not intended for external consumption
      */
    void setConnection( final StreamsConnection sc) {
        connection = sc;
    }

    /**
     * @return {@Operator}
     * @throws IOException
     */
    public List<Operator> getOperators() throws IOException {
        String sReturn = connection.getResponseString(operators);

        List<Operator> oList = new OperatorsArray(connection, sReturn).getOperators();
        return oList;
    }

    /**
     * @return true if this job is cancelled, false otherwise
     * @throws IOException
     * @throws Exception
     */
    public boolean cancel() throws Exception, IOException {
        return connection.cancelJob(id);
    }

    public String getActiveViews() {
        return activeViews;
    }

    public String getAdlFile() {
        return adlFile;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getApplicationPath() {
        return applicationPath;
    }

    public String getApplicationScope() {
        return applicationScope;
    }

    public String getApplicationVersion() {
        return applicationVersion;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public String getDataPath() {
        return dataPath;
    }

    public String getDomain() {
        return domain;
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

    public String getInstance() {
        return instance;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public String getName() {
        return name;
    }

    public String getOperatorConnections() {
        return operatorConnections;
    }

    public String getOutputPath() {
        return outputPath;
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

    public String getStartedBy() {
        return startedBy;
    }

    public String getStatus() {
        return status;
    }

    public ArrayList<String> getSubmitParameters() {
        return submitParameters;
    }

    public long getSubmitTime() {
        return submitTime;
    }

    public String getViews() {
        return views;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
