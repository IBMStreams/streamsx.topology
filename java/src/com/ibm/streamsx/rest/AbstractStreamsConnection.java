/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.http.client.fluent.Executor;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.Expose;

/**
 * Connection to IBM Streams instance
 */
abstract class AbstractStreamsConnection implements StreamsConnection {

    private static final String INSTANCES_RESOURCE_NAME = "instances";
    private static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.rest.StreamsConnection");

    private final String resourcesUrl;

    protected Executor executor;
    protected String authorization;
    protected String instancesUrl;

    abstract protected String getAuthorization();

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
        // Create the executor with a custom verifier if insecure connections
        // were requested
        try {
            if (allowInsecure) {
                // Insecure host connections were requested, try to set up
                CloseableHttpClient httpClient = HttpClients.custom()
                        .setHostnameVerifier(new AllowAllHostnameVerifier())
                        .setSslcontext(new SSLContextBuilder()
                                .loadTrustMaterial(null, new TrustStrategy() {
                                    public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                                        return true;
                                    }
                                }).build()).build();
                executor = Executor.newInstance(httpClient);
                traceLog.info("Insecure Host Connection enabled");
            } else {
                // Default, secure host connections
                executor = Executor.newInstance();
            }
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            // Insecure was requested but could not be set up
            executor = Executor.newInstance();
            traceLog.info("Could not set up Insecure Host Connection");
        }
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
        String sReturn = getResponseString(instancesUrl);
        List<Instance> instanceList = Instance.getInstanceList(this, sReturn);

        return instanceList;
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
            String response = getResponseString(query);

            List<Instance> instances = Instance.getInstanceList(this, response);
            if (instances.size() == 1) {
                // Should find one or none
                si = instances.get(0);
            } else {
                throw new RESTException("No single instance with id " + instanceId);
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
