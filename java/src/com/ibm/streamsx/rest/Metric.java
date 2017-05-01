/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

public class Metric {
    @SuppressWarnings("unused")
    private final StreamsConnection connection;
    private MetricGson metric;

    public Metric(StreamsConnection sc, MetricGson gsonMetric) {
        connection = sc;
        metric = gsonMetric;
    };

    public String getDescription() {
        return metric.description;
    }

    public long getLastTimeRetrieved() {
        return metric.lastTimeRetrieved;
    }

    public String getMetricKind() {
        return metric.metricKind;
    }

    public String getMetricType() {
        return metric.metricType;
    }

    public String getName() {
        return metric.name;
    }

    public String getResourceType() {
        return metric.resourceType;
    }

    public long getValue() {
        return metric.value;
    }

}
