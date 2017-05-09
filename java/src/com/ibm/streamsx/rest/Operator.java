/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.List;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

public class Operator {

    private StreamsConnection connection;
    @Expose
    private String connections;
    @Expose
    private String domain;
    @Expose
    private String host;
    @Expose
    private long indexWithinJob;
    @Expose
    private String inputPorts;
    @Expose
    private String instance;
    @Expose
    private String job;
    @Expose
    private String metrics;
    @Expose
    private String name;
    @Expose
    private String operatorKind;
    @Expose
    private String outputPorts;
    @Expose
    private String pe;
    @Expose
    private String resourceAllocation;
    @Expose
    private String resourceType;
    @Expose
    private String restid;
    @Expose
    private String self;

    /**
     * this function is not intended for external consumption
     */
    void setConnection(final StreamsConnection sc) {
        connection = sc;
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

    public String getConnections() {
        return connections;
    }

    public String getDomain() {
        return domain;
    }

    public String getHost() {
        return host;
    }

    public long getIndexWithinJob() {
        return indexWithinJob;
    }

    public List<InputPort> getInputPorts() throws IOException {
        String sReturn = connection.getResponseString(inputPorts);
        List<InputPort> lInPorts = new InputPortsArray(connection, sReturn).getInputPorts();
        return lInPorts;
    }

    public String getInstance() {
        return instance;
    }

    public String getJob() {
        return job;
    }

    public String getName() {
        return name;
    }

    public String getOperatorKind() {
        return operatorKind;
    }

    public List<OutputPort> getOutputPorts() throws IOException {
        String sReturn = connection.getResponseString(outputPorts);
        List<OutputPort> lOutPorts = new OutputPortsArray(connection, sReturn).getOutputPorts();
        return lOutPorts;
    }

    public String getPe() {
        return pe;
    }

    public String getResourceAllocation() {
        return resourceAllocation;
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

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
