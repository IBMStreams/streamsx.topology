/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import com.ibm.streamsx.topology.internal.streams.InvokeCancel;

/**
 * Connection to IBM Streams
 * <p>
 * This class exists for backward compatibility. Users should instead create
 * instances of {@link StreamsConnectionInterface} using the factory methods
 * in {@link StreamsRestFactory}.
 */
@Deprecated
public class StreamsConnection implements StreamsConnectionInterface {
    protected StreamsConnectionInterface delegate;
    protected boolean allowInsecure;

    private String userName;
    private String authToken;
    private String url;

    protected StreamsConnection(StreamsConnectionInterface delegate,
            boolean allowInsecure) {
        this.delegate = delegate;
        this.allowInsecure = allowInsecure;
    }

    /**
     * Connection to IBM Streams
     *
     * @param userName
     *            String representing the userName to connect to the instance
     * @param authToken
     *            String representing the password to connect to the instance
     * @param url
     *            String representing the root url to the REST API
     * @return a connection to IBM Streams
     */
    public static StreamsConnection createInstance(String userName, String authToken, String url) {
        System.err.println("createInstance start");
        StreamsConnectionInterface delegate = null;
        try {
            delegate = StreamsRestFactory.createStreamsConnection(userName,
                    authToken, url, false);
            System.err.println("createInstance delegate ok");
        } catch (IOException ioe) {
            System.err.println("createInstance delegate I/O exception");
            delegate = new InvalidStreamsConnection(ioe);
            System.err.println("createInstance delegate dummy ok");
        } catch (Exception e) {
            System.err.println("createInstance delegate exception");
            delegate = new InvalidStreamsConnection(e);
            System.err.println("createInstance delegate dummy ok");
        }
        System.err.println("createInstance delegate ready");
        StreamsConnection sc = new StreamsConnection(delegate, false);
        System.err.println("createInstance StreamsConnection ok");
        sc.userName = userName;
        sc.authToken = authToken;
        sc.url = url;
        System.err.println("createInstance end");
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
                delegate = StreamsRestFactory.createStreamsConnection(userName,
                        authToken, url, allowInsecure);
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
     * Cancels a job at this streams connection identified by the jobId
     * 
     * @param jobId
     *            string identifying the job to be cancelled
     * @return a boolean indicating
     *         <ul>
     *         <li>true if the jobId is cancelled</li>
     *         <li>false if the jobId did not get cancelled</li>
     *         </ul>
     * @throws Exception
     */
    public boolean cancelJob(String jobId) throws Exception {
        InvokeCancel cancelJob = new InvokeCancel(new BigInteger(jobId), userName);
        return cancelJob.invoke(false) == 0;
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

    // Since the original class never threw on exception on creation, we need a
    // dummy class that will throw on use.
    static class InvalidStreamsConnection implements StreamsConnectionInterface {
        private final static String MSG = "Invalid Streams connection";

        protected final Exception exception;

        protected InvalidStreamsConnection(Exception e) {
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
