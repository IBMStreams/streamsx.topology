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
 * An InputPort of the IBM Streams Operator
 */
public class InputPort {
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
    private String peInputPorts;
    @Expose
    private String resourceType;
    @Expose
    private String restid;
    @Expose
    private String self;

    private void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    static final List<InputPort> getInputPortList(StreamsConnection sc, String inputPortListString) {
        List<InputPort> ipList;
        InputPortArray ipArray;
        try {
            ipArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(inputPortListString,
                    InputPortArray.class);

            ipList = ipArray.inputPorts;
            for (InputPort ip : ipList) {
                ip.setConnection(sc);
            }
        } catch (JsonSyntaxException e) {
            ipList = Collections.<InputPort> emptyList();
        }
        return ipList;
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
        String sReturn = connection.getResponseString(metrics);
        List<Metric> sMetrics = Metric.getMetricList(connection, sReturn);
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
     * @return "inputport"
     */
    public String getResourceType() {
        return resourceType;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }

    private static class InputPortArray {
        @Expose
        private ArrayList<InputPort> inputPorts;
        @Expose
        private String resourceType;
        @Expose
        private int total;
    }

}
