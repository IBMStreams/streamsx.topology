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
 * An object describing an IBM Streams Operator
 *
 */
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
     * Gets a list of {@link Metric metrics} for this operator
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     * @throws IOException
     */
    public List<Metric> getMetrics() throws IOException {

        String sReturn = connection.getResponseString(metrics);
        List<Metric> lMetrics = new MetricsArray(connection, sReturn).getMetrics();

        return lMetrics;
    }

    /**
     * Gets the index of this operator within the {@link Job}
     * 
     * @return the index as a long
     */
    public long getIndexWithinJob() {
        return indexWithinJob;
    }

    /**
     * Gets a list of {@link InputPort input ports} for this operator
     *
     * 
     * @return List of {@link InputPort Input Ports} for this operator
     * @throws IOException
     */
    public List<InputPort> getInputPorts() throws IOException {
        String sReturn = connection.getResponseString(inputPorts);
        List<InputPort> lInPorts = new InputPortsArray(connection, sReturn).getInputPorts();
        return lInPorts;
    }

    /**
     * Name of this operator
     * 
     * @return the operator name
     */
    public String getName() {
        return name;
    }

    /**
     * SPL primitive operator type for this operator
     * 
     * @return the SPL primitive operator type
     */
    public String getOperatorKind() {
        return operatorKind;
    }

    /**
     * Gets a list of {@link OutputPort output ports} for this operator
     * 
     * @return List of {@link OutputPort Output Ports} for this operator
     * @throws IOException
     */
    public List<OutputPort> getOutputPorts() throws IOException {
        String sReturn = connection.getResponseString(outputPorts);
        List<OutputPort> lOutPorts = new OutputPortsArray(connection, sReturn).getOutputPorts();
        return lOutPorts;
    }

    /**
     * Identifies the REST resource type
     * 
     * @return "operator"
     */
    public String getResourceType() {
        return resourceType;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
