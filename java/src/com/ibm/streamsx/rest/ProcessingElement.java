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

public class ProcessingElement {

    private StreamsConnection connection;

    @Expose
    private String connections;
    @Expose
    private String currentWorkingPath;
    @Expose
    private String domain;
    @Expose
    private String health;
    @Expose
    private String host;
    @Expose
    private String id;
    @Expose
    private long indexWithinJob;
    @Expose
    private String inputPorts;
    @Expose
    private String instance;
    @Expose
    private String job;
    @Expose
    private int launchCount;
    @Expose
    private String metrics;
    @Expose
    private String operators;
    @Expose
    private String optionalConnections;
    @Expose
    private ArrayList<String> osCapabilities;
    @Expose
    private String outputPorts;
    @Expose
    private String pendingTracingLevel;
    @Expose
    private String processId;
    @Expose
    private String relocatable;
    @Expose
    private String requiredConnections;
    @Expose
    private String resourceAllocation;
    @Expose
    private ArrayList<String> resourceTags;
    @Expose
    private String resourceType;
    @Expose
    private boolean restartable;
    @Expose
    private String restid;
    @Expose
    private String self;
    @Expose
    private String status;
    @Expose
    private String statusReason;
    @Expose
    private String tracingLevel;

    /**
     * this function is not intended for external consumption
     */
    void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    /**
     * @param sc StreamsConnection to access other REST apis
     * @param peGSONList 
     * @return
     */
    final static List<ProcessingElement> getPEList(StreamsConnection sc, String peGSONList) {
        ProcessingElementArray peArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create()
                .fromJson(peGSONList, ProcessingElementArray.class);

        for (ProcessingElement pe : peArray.pes) {
            pe.setConnection(sc);
        }
        return peArray.pes;
    }

    /**
     * @return List of {@Metric}
     * @throws IOException
     */
    public List<Metric> getMetrics() throws IOException {
        String sReturn = connection.getResponseString(metrics);
        List<Metric> lMetrics = new MetricsArray(connection, sReturn).getMetrics();
        return lMetrics;
    }

    /**
     * @return List of {@InputPort}
     * @throws IOException
     */
    public List<InputPort> getInputPorts() throws IOException {
        String sReturn = connection.getResponseString(inputPorts);
        List<InputPort> lInPorts = new InputPortsArray(connection, sReturn).getInputPorts();
        return lInPorts;
    }

    /**
     * @return List of {@Operator}
     * @throws IOException
     */
    public List<Operator> getOperators() throws IOException {
        String sReturn = connection.getResponseString(operators);
        List<Operator> oList = new OperatorsArray(connection, sReturn).getOperators();
        return oList;
    }

    /**
     * @return List of {@OutputPort}
     * @throws IOException
     */
    public List<OutputPort> getOutputPorts() throws IOException {
        String sReturn = connection.getResponseString(outputPorts);
        List<OutputPort> lOutPorts = new OutputPortsArray(connection, sReturn).getOutputPorts();
        return lOutPorts;
    }

    /**
     * @return the connections
     */
    public String getConnections() {
        return connections;
    }

    /**
     * @return the currentWorkingPath
     */
    public String getCurrentWorkingPath() {
        return currentWorkingPath;
    }

    /**
     * @return the domain
     */
    public String getDomain() {
        return domain;
    }

    /**
     * @return the health
     */
    public String getHealth() {
        return health;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @return the id
     */
    public String getId() {
        return id;
    }

    /**
     * @return the indexWithinJob
     */
    public long getIndexWithinJob() {
        return indexWithinJob;
    }

    /**
     * @return the instance
     */
    public String getInstance() {
        return instance;
    }

    /**
     * @return the job
     */
    public String getJob() {
        return job;
    }

    /**
     * @return the launchCount
     */
    public int getLaunchCount() {
        return launchCount;
    }

    /**
     * @return the optionalConnections
     */
    public String getOptionalConnections() {
        return optionalConnections;
    }

    /**
     * @return the osCapabilities
     */
    public List<String> getOsCapabilities() {
        return osCapabilities;
    }

    /**
     * @return the pendingTracingLevel
     */
    public String getPendingTracingLevel() {
        return pendingTracingLevel;
    }

    /**
     * @return the processId
     */
    public String getProcessId() {
        return processId;
    }

    /**
     * @return the relocatable
     */
    public String getRelocatable() {
        return relocatable;
    }

    /**
     * @return the requiredConnections
     */
    public String getRequiredConnections() {
        return requiredConnections;
    }

    /**
     * @return the resourceAllocation
     */
    public String getResourceAllocation() {
        return resourceAllocation;
    }

    /**
     * @return the resourceTags
     */
    public List<String> getResourceTags() {
        return resourceTags;
    }

    /**
     * @return the resourceType
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * @return the restartable
     */
    public boolean getRestartable() {
        return restartable;
    }

    /**
     * @return the restid
     */
    public String getRestid() {
        return restid;
    }

    /**
     * @return the self
     */
    public String getSelf() {
        return self;
    }

    /**
     * @return the status
     */
    public String getStatus() {
        return status;
    }

    /**
     * @return the statusReason
     */
    public String getStatusReason() {
        return statusReason;
    }

    /**
     * @return the tracingLevel
     */
    public String getTracingLevel() {
        return tracingLevel;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }

    /**
     * internal usage to get the list of processing elements
     * 
     */
    private static class ProcessingElementArray {
        @Expose
        private ArrayList<ProcessingElement> pes;
        @Expose
        private String resourceType;
        @Expose
        private int total;

    }
}
