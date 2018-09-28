/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.fluent.Executor;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;

/**
 * Connection to IBM Streams instance
 */
abstract class AbstractStreamsConnection {

    private static final String INSTANCES_RESOURCE_NAME = "instances";

    private final String resourcesUrl;

    protected Executor executor;
    private String instancesUrl;
    
    /**
     * Cancel a job.
     * domainInstance will only be called for distributed where we cancel using
     * streamtool and not the context provided by the REST connection object.
     * 
     * @param instance - instance job is running in.
     * @param jobId Job identifier.
     * @return True if job was canceled.
     * @throws IOException
     */
    abstract boolean cancelJob(Instance instance, String jobId) throws IOException;
    
    ApplicationBundle uploadBundle(Instance instance, File bundle) throws IOException {
    	return new FileBundle(instance, bundle);
    }
    
    abstract Result<Job,JsonObject> submitJob(ApplicationBundle bundle, JsonObject jco) throws IOException;

    abstract String getAuthorization();

    /**
     * Connection to IBM Streams
     * 
     * @param authorization
     *            String representing Authorization header used for connections.
     * @param allowInsecure
     *            Flag to allow insecure TLS/SSL connections. This is
     *            <strong>not</strong> recommended in a production environment
     * @param resourcesUrl
     *            String representing the root url to the REST API resources,
     *            for example: https://server:port/streams/rest/resources
     */
    AbstractStreamsConnection(String resourcesUrl,
            boolean allowInsecure) {
        this.resourcesUrl = resourcesUrl;
        this.executor = StreamsRestUtils.createExecutor(allowInsecure);
    }
    
    public boolean allowInsecureHosts(boolean allowInsecure) {
    	this.executor = StreamsRestUtils.createExecutor(allowInsecure);
    	return allowInsecure;
    }

    /**
     * Direct package access to the Executor.
     */
    Executor getExecutor() {
        return executor;
    }

    /**
     * Gets a response to an HTTP call
     * 
     * @param inputString
     *            REST call to make
     * @return response from the inputString
     * @throws IOException
     */
    String getResponseString(String inputString) throws IOException {
        return StreamsRestUtils.getResponseString(executor, getAuthorization(), inputString);
    }

    /* (non-Javadoc)
     * @see com.ibm.streamsx.rest.StreamsConnection#getInstances()
     */
    public List<Instance> getInstances() throws IOException {
        return Instance.createInstanceList(this, getInstancesURL());
    }

    /* (non-Javadoc)
     * @see com.ibm.streamsx.rest.StreamsConnection#getInstance(java.lang.String)
     */
    public Instance getInstance(String instanceId) throws IOException {
        if (instanceId.isEmpty()) {
            throw new IllegalArgumentException("Empty instance id");
        } else {
            String query = getInstancesURL() + "?id=" + instanceId;

            List<Instance> instances = Instance.createInstanceList(this, query);
            if (instances.size() == 1) {
                // Should find one or none
                return instances.get(0);
            } else {
                throw new RESTException(404, "No instance with id " + instanceId);
            }
        }
    }
    
    private String getInstancesURL() throws IOException {
    	if (instancesUrl == null) {
            // Query the resourcesUrl to find the instances URL
            String response = getResponseString(resourcesUrl);
            ResourcesArray resources = new GsonBuilder()
                    .excludeFieldsWithoutExposeAnnotation()
                    .create().fromJson(response, ResourcesArray.class);
            for (Resource resource : resources.resources) {
                if (INSTANCES_RESOURCE_NAME.equals(resource.name)) {
                    instancesUrl = resource.resource;
                    break;
                }
            }
            if (null == instancesUrl) {
                // If we couldn't find instances something is wrong
                throw new RESTException("Unable to find instances resource from resources URL: " + resourcesUrl);
            }
    	}
    	return instancesUrl;
    }

    private static class Resource {
        @Expose
        public String name;

        @Expose
        public String resource;
    }

    private static class ResourcesArray {
        @Expose
        public ArrayList<Resource> resources;
    }

}
