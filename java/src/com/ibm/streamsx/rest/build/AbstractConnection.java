/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest.build;

import java.io.IOException;

import org.apache.http.client.fluent.Executor;

/**
 * Connection to IBM Streams instance
 */
abstract class AbstractConnection {

    protected Executor executor;

    abstract String getAuthorization();

    /**
     * Connection to IBM Streams
     * 
     * @param authorization
     *            String representing Authorization header used for connections.
     * @param allowInsecure
     *            Flag to allow insecure TLS/SSL connections. This is
     *            <strong>not</strong> recommended in a production environment
     */
    AbstractConnection(boolean allowInsecure) {
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
    String getResponseString(String url) throws IOException {
        return StreamsRestUtils.getResponseString(executor, getAuthorization(), url);
    }

}
