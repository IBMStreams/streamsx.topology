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
    public void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    /**
     * @return the connections
     */
    public String getConnections() {
        return connections;
    }

    /**
     * @return the indexWithinOperator
     */
    public long getIndexWithinOperator() {
        return indexWithinOperator;
    }

    /**
     * @return the job
     */
    public String getJob() {
        return job;
    }

    /**
     * @return the metrics
     */
    public List<Metric> getMetrics() throws IOException {
        String sReturn = connection.getResponseString(metrics);
        List<Metric> sMetrics = new MetricsArray(connection, sReturn).getMetrics();

        return sMetrics;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the operator
     */
    public String getOperator() {
        return operator;
    }

    /**
     * @return the pe
     */
    public String getPe() {
        return pe;
    }

    /**
     * @return the peOutputPorts
     */
    public String getPeOutputPorts() {
        return peOutputPorts;
    }

    /**
     * @return the resourceType
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * @return the restid
     */
    public String getRestid() {
        return restid;
    }

    /**
     * @return the self
     */
    public String getSelf() {
        return self;
    }

    /**
     * @return the streamName
     */
    public String getStreamName() {
        return streamName;
    };

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
