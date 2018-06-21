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
 * A Output Port of the IBM Streams Processing Element
 */
public class PEOutputPort extends Element {

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
    private String transportType;

    static final List<PEOutputPort> createOutputPortList(AbstractStreamsConnection sc,
            String uri) throws IOException {
       return createList(sc, uri, PEOutputPortArray.class);
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
        List<Metric> sMetrics = Metric.getMetricList(connection(), metrics);

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

    private static class PEOutputPortArray extends ElementArray<PEOutputPort> {
        @Expose
        private ArrayList<PEOutputPort> outputPorts;
        
        @Override
        List<PEOutputPort> elements() { return outputPorts; }
    }
}
