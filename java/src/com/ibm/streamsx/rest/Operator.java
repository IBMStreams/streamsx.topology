/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.annotations.Expose;

/**
 * An object describing an IBM Streams Operator
 *
 */
public class Operator extends Element {

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
    private String logicalName;
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

    static final List<Operator> createOperatorList(AbstractStreamsConnection sc,
             String uri) throws IOException {
        return createList(sc, uri, OperatorArray.class);
    }

    /**
     * The logical name of this operator.
     * 
     * @return the logical name of the operator, which is just the name if the
     * operator is not part of a parallel region.
     * @since 1.9
     */
    public String getLogicalName() {
        return logicalName == null ? name : logicalName;
    }

    /**
     * Gets a list of {@link Metric metrics} for this operator
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     * @throws IOException
     */
    public List<Metric> getMetrics() throws IOException {

        List<Metric> lMetrics = Metric.getMetricList(connection(), metrics);

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
        return InputPort.createInputPortList(connection(), inputPorts);
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
        return OutputPort.createOutputPortList(connection(), outputPorts);
    }

    /**
     * Identifies the REST resource type
     * 
     * @return "operator"
     */
    public String getResourceType() {
        return resourceType;
    }
    
    /**
     * Get the PE for this operator.
     * @return PE for this operator.
     * @throws IOException
     * 
     * @since 1.9
     */
    public ProcessingElement getPE() throws IOException {
        return create(connection(), pe, ProcessingElement.class);
    }
    
    private static class OperatorArray extends ElementArray<Operator> {
        @Expose
        private ArrayList<Operator> operators;
        
        List<Operator> elements() { return operators; }
    }

}
