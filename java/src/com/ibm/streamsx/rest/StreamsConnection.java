/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import org.apache.http.client.fluent.Executor;

import com.ibm.streamsx.topology.internal.streams.InvokeCancel;

/**
 * Connection to IBM Streams.
 */
public class StreamsConnection implements IStreamsConnection {
    protected IStreamsConnection delegate;
    protected boolean allowInsecure;

    private String userName;
    private String authToken;
    private String url;

    /**
     * Direct use by derived classes is strongly discouraged. May be stale or
     * expired. Invoke a member method to refresh.
     */
    protected String apiKey;
    protected Executor executor;

    StreamsConnection(IStreamsConnection delegate,
            boolean allowInsecure) {
        this.delegate = delegate;
        this.allowInsecure = allowInsecure;
        refreshState();
    }

    protected StreamsConnection(String userName, String authToken, String url) {
        this(createDelegate(userName, authToken, url), false);
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
    public static StreamsConnection createInstance(String userName,
            String authToken, String url) {
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
        refreshState();
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
        refreshState();
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
        refreshState();
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
        refreshState();
        return delegate.getInstances();
    }

    // Refresh protected members from the previous implementation
    void refreshState() {
        if (delegate instanceof AbstractStreamsConnection) {
            AbstractStreamsConnection asc = (AbstractStreamsConnection)delegate;
            apiKey = asc.getAuthorization();
            executor = asc.getExecutor();
        }
    }

    protected void setStreamsRESTURL(String url) {
        delegate = createDelegate(userName, authToken, url);
        refreshState();
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
