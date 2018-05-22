/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.List;

import com.google.gson.annotations.Expose;

/**
 * Domain resource information.
 * 
 * A resource that is available for running Streams services and applications.
 * 
 * @since 1.9
 */
public class Resource extends Element {

    @Expose
    private String id;
    @Expose
    private String metrics;
    @Expose
    private String resourceType;
    @Expose
    private String restid;
    
    @Expose
    private String ipAddress;
    @Expose
    private String displayName;
    @Expose
    private String status;

    /**
     * Get the unique Streams identifier for this resource.
     * The identifier is unique within a domain.
     * @return Streams identifier for this resource.
     */
    public String getId() {
        return id;
    }
    
    /**
     * Get the IP address for this resource.
     * @return IP address for this resource.
     */
    public String getIpAddress() {
        return ipAddress;
    }

    /**
     * Get the display name for this resource.
     * @return Display name for this resource.
     */
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Status of the resource.
     * 
     * Some possible values for this property include
     * {@code failed}, {@code partiallyFailed}, {@code partiallyRunning},
     * {@code quiesced}, {@code quiescing}, {@code running},
     * {@code restarting}, {@code resuming}, {@code starting}, 
     * {@code stopped}, and {@code unknown}.
     * 
     * @return Status of the resource.
     */
    public String getStatus() {
        return status;
    }

    /**
     * Gets the {@link Metric metrics} for this resource.
     * 
     * @return List of {@link Metric IBM Streams Metrics}
     */
    public List<Metric> getMetrics() throws IOException {
        return Metric.getMetricList(connection(), metrics);
    }
    
    /**
     * Identifies the REST resource type
     * 
     * @return "resource"
     */
    public String getResourceType() {
        return resourceType;
    }

}
