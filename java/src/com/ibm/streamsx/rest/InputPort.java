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
 * An Input Port of the IBM Streams Operator
 */
public class InputPort extends Element {

    @Expose
    private String connections;
    @Expose
    long indexWithinOperator;
    @Expose
    private String job;
    @Expose
    private String metrics;
    @Expose
    private String name;
    @Expose
    private String operator;
    @Expose
    private String pe;
    @Expose
    private String peInputPorts;
    @Expose
    private String resourceType;
    @Expose
    private String restid;

    static final List<InputPort> createInputPortList(AbstractStreamsConnection sc,
            String uri) throws IOException {
        return createList(sc, uri, InputPortArray.class);
    }

    /**
     * Gets the index of this input port within the {@link Operator}
     * 
     * @return the index number as a long
     */
    public long getIndexWithinOperator() {
        return indexWithinOperator;
    }

    /**
     * Gets the {@link Metric metrics} for this input port
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     */
    public List<Metric> getMetrics() throws IOException {
        
        List<Metric> sMetrics = Metric.getMetricList(connection(), metrics);
        return sMetrics;
    }

    /**
     * Gets the name for this input port
     * 
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Identifies the REST resource type
     * 
     * @return "operatorInputPort"
     */
    public String getResourceType() {
        return resourceType;
    }

    private static class InputPortArray extends ElementArray<InputPort> {
        @Expose
        private ArrayList<InputPort> inputPorts;
        @Override
        List<InputPort> elements() { return inputPorts; }
    }
}
