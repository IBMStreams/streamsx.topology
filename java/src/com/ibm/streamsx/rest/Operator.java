/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
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

    private void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    static final List<Operator> getOperatorList(StreamsConnection sc, String operatorsList) {
        List<Operator> opList;
        OperatorArray opArray;
        try {
            opArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(operatorsList,
                    OperatorArray.class);

            opList = opArray.operators;
            for (Operator op : opList) {
                op.setConnection(sc);
            }
        } catch (JsonSyntaxException e) {
            opList = Collections.<Operator> emptyList();
        }
        return opList;
    }

    /**
     * Gets a list of {@link Metric metrics} for this operator
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     * @throws IOException
     */
    public List<Metric> getMetrics() throws IOException {

        String sReturn = connection.getResponseString(metrics);
        List<Metric> lMetrics = Metric.getMetricList(connection, sReturn);

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
        List<InputPort> lInPorts = InputPort.getInputPortList(connection, sReturn);
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
        List<OutputPort> lOutPorts = OutputPort.getOutputPortList(connection, sReturn);
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

    private static class OperatorArray {
        @Expose
        private ArrayList<Operator> operators;
        @Expose
        private String resourceType;
        @Expose
        private int total;
    }

}
