/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.fluent.Executor;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

/**
 * Connection to IBM Streams instance
 */
abstract class AbstractStreamsConnection implements IStreamsConnection {

    private static final String INSTANCES_RESOURCE_NAME = "instances";

    private final String resourcesUrl;

    protected Executor executor;
    protected String authorization;
    protected String instancesUrl;

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
    AbstractStreamsConnection(String authorization, String resourcesUrl,
            boolean allowInsecure) throws IOException {
        this.authorization = authorization;
        this.resourcesUrl = resourcesUrl;
        this.executor = StreamsRestUtils.createExecutor(allowInsecure);
    }

    /**
     * Must be called after construction once to initialize the connection.
     */
    synchronized void init() throws IOException, JsonSyntaxException {
        if (null == instancesUrl) {
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
    }

    /**
     * Direct package access to the Executor.
     */
    Executor getExecutor() {
        return executor;
    }

    /**
     * Set the contents of the authorization header
     */
    protected void setAuthorization(String authorization) {
        this.authorization = authorization;
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
    @Override
    public List<Instance> getInstances() throws IOException {
        return Instance.createInstanceList(this, instancesUrl);
    }

    /* (non-Javadoc)
     * @see com.ibm.streamsx.rest.StreamsConnection#getInstance(java.lang.String)
     */
    @Override
    public Instance getInstance(String instanceId) throws IOException {
        Instance si = null;
        if ("".equals(instanceId)) {
            // should add some fallback code to see if there's only one instance
            throw new IllegalArgumentException("Missing instance id");
        } else {
            String query = instancesUrl + "?id=" + instanceId;

            List<Instance> instances = Instance.createInstanceList(this, query);
            if (instances.size() == 1) {
                // Should find one or none
                si = instances.get(0);
            } else {
                throw new RESTException(404, "No single instance with id " + instanceId);
            }

        }
        return si;
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
