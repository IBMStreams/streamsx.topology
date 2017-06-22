/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

/**
 * An Input Port of the IBM Streams Processing Element
 */
public class PEInputPort extends Element {

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
    private String transportType;

    static final List<PEInputPort> getInputPortList(StreamsConnection sc, String inputPortListString) {
        List<PEInputPort> ipList;
        try {
            PEInputPortArray ipArray = gson.fromJson(inputPortListString, PEInputPortArray.class);

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
        List<Metric> sMetrics = Metric.getMetricList(connection(), metrics);
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

    private static class PEInputPortArray {
        @Expose
        private ArrayList<PEInputPort> inputPorts;
        @Expose
        private String resourceType;
        @Expose
        private int total;
    }

}
