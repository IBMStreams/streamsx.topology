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
 * An Output Port of the IBM Streams Operator
 */
public class OutputPort extends Element {

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
    private String peOutputPorts;
    @Expose
    private String resourceType;
    @Expose
    private String restid;
    @Expose
    private String streamName;

    static final List<OutputPort> createOutputPortList(AbstractStreamsConnection sc,
            String uri) throws IOException {       
        return createList(sc, uri, OutputPortArray.class);
    }

    /**
     * Gets the index of this output port within the {@link Operator operator}
     * 
     * @return the index as a long
     */
    public long getIndexWithinOperator() {
        return indexWithinOperator;
    }

    /**
     * Gets the {@link Metric metrics} for this output port
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     */
    public List<Metric> getMetrics() throws IOException {

        List<Metric> sMetrics = Metric.getMetricList(connection(), metrics);

        return sMetrics;
    }

    /**
     * Gets the name for this output port
     * 
     * @return the output port name
     */
    public String getName() {
        return name;
    }

    /**
     * Identifies the REST resource type
     * 
     * @return "operatorOutputPort"
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Identifies the name of the output stream associated with this output port
     * 
     * @return the output stream name for this port
     */
    public String getStreamName() {
        return streamName;
    };

    private static class OutputPortArray extends ElementArray<OutputPort> {
        @Expose
        private ArrayList<OutputPort> outputPorts;
        
        @Override
        List<OutputPort> elements() {return outputPorts;}
    }
}
