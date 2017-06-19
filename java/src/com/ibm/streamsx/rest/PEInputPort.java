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
 * An Input Port of the IBM Streams Processing Element
 */
public class PEInputPort {

    private StreamsConnection connection;

    @Expose
    private String connections;
    @Expose
    private long indexWithinPE;
    @Expose
    private String id;
    @Expose
    private String job;
    @Expose
    private String metrics;
    @Expose
    private String pe;
    @Expose
    private String resourceType;
    @Expose
    private String restid;
    @Expose
    private String self;
    @Expose
    private String transportType;

    private void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    static final List<PEInputPort> getInputPortList(StreamsConnection sc, String inputPortListString) {
        List<PEInputPort> ipList;
        PEInputPortArray ipArray;
        try {
            ipArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(inputPortListString,
                    PEInputPortArray.class);

            ipList = ipArray.inputPorts;
            for (PEInputPort ip : ipList) {
                ip.setConnection(sc);
            }
        } catch (JsonSyntaxException e) {
            ipList = Collections.<PEInputPort> emptyList();
        }
        return ipList;
    }

    /**
     * Gets the index of this input port within the {@link ProcessingElement
     * processing element}
     * 
     * @return the index number as a long
     */
    public long getIndexWithinPE() {
        return indexWithinPE;
    }

    /**
     * Gets the {@link Metric metrics} for this processing element input port
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     */
    public List<Metric> getMetrics() throws IOException {
        String sReturn = connection.getResponseString(metrics);
        List<Metric> sMetrics = Metric.getMetricList(connection, sReturn);
        return sMetrics;
    }

    /**
     * Gets the transport type for this processing element input port
     * 
     * @return the transport type containing one of the following possible
     *         values:
     *         <ul>
     *         <li>tcp</li>
     *         <li>llm-rum-tcp</li>
     *         <li>llm-rum-ib</li>
     *         </ul>
     */
    public String getTransportType() {
        return transportType;
    }

    /**
     * Identifies the REST resource type
     * 
     * @return "peInputPort"
     */
    public String getResourceType() {
        return resourceType;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }

    private static class PEInputPortArray {
        @Expose
        private ArrayList<PEInputPort> inputPorts;
        @Expose
        private String resourceType;
        @Expose
        private int total;
    }

}
