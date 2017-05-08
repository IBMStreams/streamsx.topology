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
    private StreamsConnection connection;

    @Expose 
    private String description;
    @Expose 
    private long lastTimeRetrieved;
    @Expose 
    private String metricKind;
    @Expose 
    private String metricType;
    @Expose 
    private String name;
    @Expose 
    private String resourceType;
    @Expose 
    private long value;

    /**
      * this function is not intended for external consumption
      */
    void setConnection(final StreamsConnection sc) {
        connection = sc;
    }

    public String getDescription() {
        return description;
    }

    public long getLastTimeRetrieved() {
        return lastTimeRetrieved;
    }

    public String getMetricKind() {
        return metricKind;
    }

    public String getMetricType() {
        return metricType;
    }

    public String getName() {
        return name;
    }

    public String getResourceType() {
        return resourceType;
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString() {
        return (new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create().toJson(this));
    }
}
