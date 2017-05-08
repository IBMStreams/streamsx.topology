/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * Package class to hold information all metrics information from the GET metrics URL
 */
class MetricsArray {
    private MetricsArrayGson metricsArray;

    public MetricsArray(StreamsConnection sc, String gsonMetrics) {
        metricsArray = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create().fromJson(gsonMetrics, MetricsArrayGson.class);
        for (Metric m: metricsArray.metrics) {
          m.setConnection(sc) ;
        }
    };

    /**
     * @return List of {@Metric}
     */
    public List<Metric> getMetrics() {
        return metricsArray.metrics;
    }

    private static class MetricsArrayGson {
        @Expose
        public ArrayList<Metric> metrics;
        @Expose
        public String owner;
        @Expose
        public String resourceType;
        @Expose
        public long total;
    }

    public String getOwner() {
        return metricsArray.owner;
    }

    public String getResourceType() {
        return metricsArray.resourceType;
    }

    public long getTotal() {
        return metricsArray.total;
    }

}
