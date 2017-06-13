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
 * A Output Port of the IBM Streams Processing Element
 */
public class PEOutputPort {

    private StreamsConnection connection;

    @Expose
    private String connections;
    @Expose
    long indexWithinPE;
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

    static final List<PEOutputPort> getOutputPortList(StreamsConnection sc, String outputPortList) {
        List<PEOutputPort> opList;
        PEOutputPortArray opArray;
        try {
            opArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(outputPortList,
                    PEOutputPortArray.class);

            opList = opArray.outputPorts;
            for (PEOutputPort op : opList) {
                op.setConnection(sc);
            }
        } catch (JsonSyntaxException e) {
            opList = Collections.<PEOutputPort> emptyList();
        }
        return opList;
    }

    /**
     * Gets the index of this output port within the {@link ProcessingElement
     * processing element}
     * 
     * @return the index as a long
     */
    public long getIndexWithinPE() {
        return indexWithinPE;
    }

    /**
     * Gets the {@link Metric metrics} for this output port
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     */
    public List<Metric> getMetrics() throws IOException {
        String sReturn = connection.getResponseString(metrics);
        List<Metric> sMetrics = Metric.getMetricList(connection, sReturn);

        return sMetrics;
    }

    /**
     * Gets the transport type for this processing element output port
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
     * @return "peOutputPort"
     */
    public String getResourceType() {
        return resourceType;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }

    private static class PEOutputPortArray {
        @Expose
        private ArrayList<PEOutputPort> outputPorts;
        @Expose
        private String resourceType;
        @Expose
        private int total;
    }

}
