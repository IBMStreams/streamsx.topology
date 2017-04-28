/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.primitives;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.primitives.Metric;
import com.ibm.streamsx.rest.primitives.MetricGson;

public class MetricsArray {
    private final StreamsConnection connection;
    private List<Metric> metrics;
    private MetricsArrayGson metricsArray;

    public MetricsArray(StreamsConnection sc, String gsonMetrics) {
        connection = sc;
        metricsArray = new Gson().fromJson(gsonMetrics, MetricsArrayGson.class);

        metrics = new ArrayList<Metric>(metricsArray.metrics.size());
        for (MetricGson mg : metricsArray.metrics) {
            metrics.add(new Metric(sc, mg));
        }
    };

    /**
     * @return List of {@Metric}
     */
    public List<Metric> getMetrics() {
        return metrics;
    }

    private static class MetricsArrayGson {
        public ArrayList<MetricGson> metrics;
        public String owner;
        public String resourceType;
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
