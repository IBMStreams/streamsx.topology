/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

public class Job {
    private final StreamsConnection connection;
    private JobGson job;

    public Job(StreamsConnection sc, JobGson gsonJob) {
        connection = sc;
        job = gsonJob;
    };

    public Job(StreamsConnection sc, String sJob) {
        connection = sc;
        job = new Gson().fromJson(sJob, JobGson.class);
    }

    /**
     * @return {@Operator}
     * @throws IOException
     */
    public List<Operator> getOperators() throws IOException {
        String sGetOperatorsURI = job.operators;

        String sReturn = connection.getResponseString(sGetOperatorsURI);

        List<Operator> operators = new OperatorsArray(connection, sReturn).getOperators();
        return operators;
    }

    public String getActiveViews() {
        return job.activeViews;
    }

    public String getAdlFile() {
        return job.adlFile;
    }

    public String getApplicationName() {
        return job.applicationName;
    }

    public String getApplicationPath() {
        return job.applicationPath;
    }

    public String getApplicationScope() {
        return job.applicationScope;
    }

    public String getApplicationVersion() {
        return job.applicationVersion;
    }

    public String getCheckpointPath() {
        return job.checkpointPath;
    }

    public String getDataPath() {
        return job.dataPath;
    }

    public String getDomain() {
        return job.domain;
    }

    public String getHealth() {
        return job.health;
    }

    public String getHosts() {
        return job.hosts;
    }

    public String getId() {
        return job.id;
    }

    public String getInstance() {
        return job.instance;
    }

    public String getJobGroup() {
        return job.jobGroup;
    }

    public String getName() {
        return job.name;
    }

    public String getOperatorConnections() {
        return job.operatorConnections;
    }

    public String getOutputPath() {
        return job.outputPath;
    }

    public String getPeConnections() {
        return job.peConnections;
    }

    public String getPes() {
        return job.pes;
    }

    public String getResourceAllocations() {
        return job.resourceAllocations;
    }

    public String getResourceType() {
        return job.resourceType;
    }

    public String getRestid() {
        return job.restid;
    }

    public String getSelf() {
        return job.self;
    }

    public String getStartedBy() {
        return job.startedBy;
    }

    public String getStatus() {
        return job.status;
    }

    public ArrayList<String> getSubmitParameters() {
        return job.submitParameters;
    }

    public long getSubmitTime() {
        return job.submitTime;
    }

    public String getViews() {
        return job.views;
    }

}
