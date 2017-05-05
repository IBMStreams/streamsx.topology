/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

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

    @Override
    public String toString() {
       return (new GsonBuilder().setPrettyPrinting().create().toJson(metric)) ;
    }
}
