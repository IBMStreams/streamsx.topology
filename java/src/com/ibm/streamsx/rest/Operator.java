/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.List;

public class Operator {
    private final StreamsConnection connection;
    private OperatorGson operator;

    public Operator(StreamsConnection sc, OperatorGson gsonOperator) {
        connection = sc;
        operator = gsonOperator;
    };

    /**
     * @return List of {@Metric}
     * @throws IOException
     */
    public List<Metric> getMetrics() throws IOException {
        String sGetMetricsURI = operator.metrics;

        String sReturn = connection.getResponseString(sGetMetricsURI);
        List<Metric> sMetrics = new MetricsArray(connection, sReturn).getMetrics();

        return sMetrics;
    }

    public String getConnections() {
        return operator.connections;
    }

    public String getDomain() {
        return operator.domain;
    }

    public String getHost() {
        return operator.host;
    }

    public long getIndexWithinJob() {
        return operator.indexWithinJob;
    }

    public String getInputPorts() {
        return operator.inputPorts;
    }

    public String getInstance() {
        return operator.instance;
    }

    public String getJob() {
        return operator.job;
    }

    public String getName() {
        return operator.name;
    }

    public String getOperatorKind() {
        return operator.operatorKind;
    }

    public String getOutputPorts() {
        return operator.outputPorts;
    }

    public String getPe() {
        return operator.pe;
    }

    public String getResourceAllocation() {
        return operator.resourceAllocation;
    }

    public String getResourceType() {
        return operator.resourceType;
    }

    public String getRestid() {
        return operator.restid;
    }

    public String getSelf() {
        return operator.self;
    }

}
