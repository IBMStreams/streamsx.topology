/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

/**
 * An object describing an IBM Streams Metric
 *
 */
public class Metric extends Element {

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

    static final List<Metric> getMetricList(AbstractStreamsConnection sc, String uri) throws IOException {
        
        String metricsList = sc.getResponseString(uri);
        List<Metric> mList;
        try {
            MetricArray mArray = gson.fromJson(metricsList, MetricArray.class);

            mList = mArray.metrics;
            for (Metric m : mList) {
                m.setConnection(sc, uri + "?name=" + URLEncoder.encode(m.getName(), "UTF-8"));
            }
        } catch (JsonSyntaxException e) {
            mList = Collections.<Metric> emptyList();
        } catch (UnsupportedEncodingException e) {
            // Should not happen!
            throw new RuntimeException(e);
        }
        return mList;
    }

    /**
     * Gets the description for this metric
     * 
     * @return the metric description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Gets the Epoch time when the metric was most recently retrieved
     * 
     * @return the epoch time when the metric was most recently retrieved as a
     *         long
     */
    public long getLastTimeRetrieved() {
        return lastTimeRetrieved;
    }

    /**
     * Describes the kind of metric that has been retrieved
     * 
     * @return the metric kind that contains one of the following values:
     *         <ul>
     *         <li>counter</li>
     *         <li>gauge</li>
     *         <li>time</li>
     *         <li>unknown</li>
     *         </ul>
     */
    public String getMetricKind() {
        return metricKind;
    }

    /**
     * Describes the type of metric that has been retrieved
     * 
     * @return the metric type that contains one of the following values:
     *         <ul>
     *         <li>system</li>
     *         <li>custom</li>
     *         <li>unknown</li>
     *         </ul>
     */
    public String getMetricType() {
        return metricType;
    }

    /**
     * Gets the name of this metric
     * 
     * @return the metric name
     */
    public String getName() {
        return name;
    }

    /**
     * Identifies the REST resource type
     * 
     * @return "metric"
     */
    public String getResourceType() {
        return resourceType;
    }

    /**
     * Gets the value for this metric
     * 
     * @return the metric value as a long
     */
    public long getValue() {
        return value;
    }
    
    @Override
    public void refresh() throws IOException {
        List<Metric> updates = getMetricList(connection(), self());
        if (!updates.isEmpty()) {
            Metric update = updates.get(0);
            value = update.getValue();
            lastTimeRetrieved = update.getLastTimeRetrieved();
        }
    }

    private static class MetricArray {
        @Expose
        public ArrayList<Metric> metrics;
        @Expose
        public String owner;
        @Expose
        public String resourceType;
        @Expose
        public long total;
    }

}
