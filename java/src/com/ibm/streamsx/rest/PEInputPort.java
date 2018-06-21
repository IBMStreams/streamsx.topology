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

    static final List<PEInputPort> createInputPortList(AbstractStreamsConnection sc,
            String uri) throws IOException {
        return createList(sc, uri, PEInputPortArray.class);
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

    private static class PEInputPortArray extends ElementArray<PEInputPort> {
        @Expose
        private ArrayList<PEInputPort> inputPorts;
        @Override
        List<PEInputPort> elements() { return inputPorts; }
    }

}
