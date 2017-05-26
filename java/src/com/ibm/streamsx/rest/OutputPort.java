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
 * An OutputPort of the IBM Streams Operator
 */
public class OutputPort {
    @SuppressWarnings("unused")
    private StreamsConnection connection;

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
    private String self;
    @Expose
    private String streamName;

    /**
     * this function is not intended for external consumption
     */
    void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    /**
     * Gets the index of this output port within the {@link Operator}
     * 
     * @return long
     */
    public long getIndexWithinOperator() {
        return indexWithinOperator;
    }

    /**
     * Gets the {@link Metric metrics} for this output port
     * 
     * @return {@link List} of {@link Metric}
     */
    public List<Metric> getMetrics() throws IOException {
        String sReturn = connection.getResponseString(metrics);
        List<Metric> sMetrics = new MetricsArray(connection, sReturn).getMetrics();

        return sMetrics;
    }

    /**
     * Gets the name for this output port
     * 
     * @return {@link String}
     */
    public String getName() {
        return name;
    }

    /**
     * Identifies the REST resource type, which is "outputport"
     * 
     * @return {@link String}
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Identifies the name of the output stream associated with this output port
     * 
     * @return {@link String}
     */
    public String getStreamName() {
        return streamName;
    };

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
