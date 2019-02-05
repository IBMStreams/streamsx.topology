/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018
 */
package com.ibm.streamsx.rest;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import com.ibm.streamsx.topology.internal.streams.Util;

/**
 * Connection to IBM Streams.
 */
public class StreamsConnection {
    private AbstractStreamsConnection delegate;

    StreamsConnection(AbstractStreamsConnection delegate) {
        this.delegate = delegate;
    }
    
    private final AbstractStreamsConnection delegate() {
    	return this.delegate;
    }

    /**
     * Create a connection to IBM Streams REST API.
     *
     * @param userName
     *            String representing the user name to connect to the instance.
     *            If {@code null} user name defaults to value of the environment
     *            variable {@code STREAMS_USERNAME} if set, else the value of
     *            the Java system property {@code user.name}.
     * @param authToken
     *            String representing the password to connect to the instance.
     *            If {@code null} password defaults to value of the environment
     *            variable {@code STREAMS_PASSWORD} if set.
     * @param url
     *            String representing the root url to the REST API.
     *            If {@code null} password defaults to value of the environment
     *            variable {@code STREAMS_REST_URL} if set.
     *
     * @return a connection to IBM Streams
     */
    public static StreamsConnection createInstance(String userName,
            String authToken, String url) {
    	if (userName == null) {
    		userName = System.getenv(Util.STREAMS_USERNAME);
    		if (userName == null)
    			userName = System.getProperty("user.name");
    	}
    	
    	if (authToken == null) {
    		authToken = System.getenv(Util.STREAMS_PASSWORD);
    		Objects.requireNonNull(authToken, "Environment variable " + Util.STREAMS_PASSWORD + " is not set");
    	}
    	
    	if (url == null) {
    		url = System.getenv(Util.STREAMS_REST_URL);
    		Objects.requireNonNull(url, "Environment variable " + Util.STREAMS_REST_URL + " is not set");
    	}
    	
    	AbstractStreamsConnection delegate = createDelegate(userName, authToken, url);
        StreamsConnection sc = new StreamsConnection(delegate);
        return sc;
    }
    
    public static StreamsConnection ofBearerToken(String url, String bearerToken) {
        
        AbstractStreamsConnection delegate = new StreamsConnectionImpl(null,
                StreamsRestUtils.createBearerAuth(bearerToken),
                url, false);
        StreamsConnection sc = new StreamsConnection(delegate);
        return sc;      
    }

    /**
     * This function is used to disable checking the trusted certificate chain
     * and should never be used in production environments
     * 
     * @param allowInsecure
     *            <ul>
     *            <li>true - disables checking</li>
     *            <li>false - enables checking (default)</li>
     *            </ul>
     * @return a boolean indicating the state of the connection after this
     *         method was called.
     *         <ul>
     *         <li>true - if checking is disabled</li>
     *         <li>false - if checking is enabled</li>
     *         </ul>
     */
    public boolean allowInsecureHosts(boolean allowInsecure) {
    	return delegate().allowInsecureHosts(allowInsecure);
    }

    /**
     * Gets a specific {@link Instance instance} identified by the instanceId at
     * this IBM Streams connection
     * 
     * @param instanceId
     *            name of the instance to be retrieved
     * @return a single {@link Instance}
     * @throws IOException
     */
    public Instance getInstance(String instanceId) throws IOException {   	
        return delegate().getInstance(requireNonNull(instanceId));
    }

    /**
     * Gets a list of {@link Instance instances} that are available to this IBM
     * Streams connection
     * 
     * @return List of {@link Instance IBM Streams Instances} available to this
     *         connection
     * @throws IOException
     */
    public List<Instance> getInstances() throws IOException {
        return delegate().getInstances();
    }


    private static AbstractStreamsConnection createDelegate(String userName,
            String authToken, String url) {
        return new StreamsConnectionImpl(userName,
                    StreamsRestUtils.createBasicAuth(userName, authToken),
                    url, false);
    }
}
