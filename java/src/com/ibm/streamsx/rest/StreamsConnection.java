/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.util.List;

import com.ibm.streamsx.topology.internal.streams.Util;

/**
 * Connection to IBM Streams.
 */
public class StreamsConnection {
    IStreamsConnection delegate;
    protected boolean allowInsecure;

    private String userName;
    private String authToken;
    private String url;

    StreamsConnection(IStreamsConnection delegate,
            boolean allowInsecure) {
        this.delegate = delegate;
        this.allowInsecure = allowInsecure;
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
    	
    	if (authToken == null)
    		authToken = System.getenv(Util.STREAMS_PASSWORD);
    	
    	if (url == null)
    		url = System.getenv(Util.STREAMS_REST_URL);
    	
        IStreamsConnection delegate = createDelegate(userName, authToken, url);
        StreamsConnection sc = new StreamsConnection(delegate, false);
        sc.userName = userName;
        sc.authToken = authToken;
        sc.url = url;
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
        if (allowInsecure != this.allowInsecure
                && null != userName && null != authToken && null != url) {
            try {
                StreamsConnectionImpl connection = new StreamsConnectionImpl(userName,
                        StreamsRestUtils.createBasicAuth(userName, authToken),
                        url, allowInsecure);
                connection.init();
                delegate = connection;
                this.allowInsecure = allowInsecure; 
            } catch (IOException e) {
                // Don't change current allowInsecure but update delegate in
                // case new exception is more informative.
                delegate = new InvalidStreamsConnection(e);
            }
        }
        return this.allowInsecure;
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
        return delegate.getInstance(instanceId);
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
        return delegate.getInstances();
    }


    private static IStreamsConnection createDelegate(String userName,
            String authToken, String url) {
        IStreamsConnection delegate = null;
        try {
            StreamsConnectionImpl connection = new StreamsConnectionImpl(userName,
                    StreamsRestUtils.createBasicAuth(userName, authToken),
                    url, false);
            connection.init();
            delegate = connection;
        } catch (Exception e) {
            delegate = new InvalidStreamsConnection(e);
        }
        return delegate;
    }

    // Since the original class never threw on exception on creation, we need a
    // dummy class that will throw on use.
    static class InvalidStreamsConnection implements IStreamsConnection {
        private final static String MSG = "Invalid Streams connection";

        private final Exception exception;

        InvalidStreamsConnection(Exception e) {
            exception = e;
        }

        @Override
        public Instance getInstance(String instanceId) throws IOException {
            throw new RESTException(MSG, exception);
        }

        @Override
        public List<Instance> getInstances() throws IOException {
            throw new RESTException(MSG, exception);
        }
    }
}
