/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.ibm.streamsx.topology.internal.streams.InvokeCancel;

/**
 * Connection to IBM Streams
 */
public class StreamsConnection {

    static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.rest.StreamsConnection");

    private final String userName;
    private String url;
    protected String apiKey;
    private boolean allowInsecureHosts = false;

    protected Executor executor;

    /**
     * Connection to IBM Streams
     * 
     * @param userName
     *            String representing the userName to connect to the instance
     * @param authToken
     *            String representing the password to connect to the instance
     * @param url
     *            String representing the root url to the REST API, for example:
     *            https:server:port/streams/rest
     */
    protected StreamsConnection(String userName, String authToken, String url) {
        this.userName = userName;
        String apiCredentials = userName + ":" + authToken;
        apiKey = "Basic " + DatatypeConverter.printBase64Binary(apiCredentials.getBytes(StandardCharsets.UTF_8));

        executor = Executor.newInstance();
        setStreamsRESTURL(url);
    }

    /**
     * sets the REST API url for this connection removing the trailing slash
     * 
     * @param url
     *            String representing the root url to the REST API, for example:
     *            https:server:port/streams/rest
     */
    protected void setStreamsRESTURL(String url) {
        if (url.equals("") || (url.charAt(url.length() - 1) != '/')) {
            this.url = url;
        } else {
            this.url = url.substring(0, url.length() - 1);
        }
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
        return new StreamsConnection(userName, authToken, url);
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
        String sReturn = "";
        Request request = Request.Get(inputString).addHeader(AUTH.WWW_AUTH_RESP, apiKey).useExpectContinue();

        Response response = executor.execute(request);
        HttpResponse hResponse = response.returnResponse();
        int rcResponse = hResponse.getStatusLine().getStatusCode();

        if (HttpStatus.SC_OK == rcResponse) {
            sReturn = EntityUtils.toString(hResponse.getEntity());
        } else if (HttpStatus.SC_NOT_FOUND == rcResponse) {
            // with a 404 message, we are likely to have a message from Streams
            sReturn = EntityUtils.toString(hResponse.getEntity());
            throw RESTException.create(rcResponse, sReturn);
        } else {
            // all other errors...
            String httpError = "HttpStatus is " + rcResponse + " for url " + inputString;
            throw new RESTException(rcResponse, httpError);
        }
        traceLog.finest("Request: " + inputString);
        traceLog.finest(rcResponse + ": " + sReturn);
        return sReturn;
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
        String instancesURL = url + "/instances/";

        String sReturn = getResponseString(instancesURL);
        List<Instance> instanceList = Instance.getInstanceList(this, sReturn);

        return instanceList;
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
        Instance si = null;
        if (instanceId.equals("")) {
            // should add some fallback code to see if there's only one instance
            throw new IllegalArgumentException("Missing instance name");
        } else {
            String instanceURL = url + "/instances/" + instanceId;
            String sReturn = getResponseString(instanceURL);

            si = Instance.create(this, sReturn);
        }
        return si;
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
        try {
            if ((allowInsecure) && (false == allowInsecureHosts)) {
                CloseableHttpClient httpClient = HttpClients.custom()
                        .setHostnameVerifier(new AllowAllHostnameVerifier())
                        .setSslcontext(new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                            public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                                return true;
                            }
                        }).build()).build();
                executor = Executor.newInstance(httpClient);
                allowInsecureHosts = true;
            } else if ((false == allowInsecure) && (true == allowInsecureHosts)) {
                executor = Executor.newInstance();
                allowInsecureHosts = false;
            }
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            executor = Executor.newInstance();
            allowInsecureHosts = false;
        }
        if (allowInsecureHosts) {
            traceLog.info("Insecure Host Connection enabled");
        }
        return allowInsecureHosts;
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
        boolean rc = true;
        InvokeCancel cancelJob = new InvokeCancel(new BigInteger(jobId), userName);
        cancelJob.invoke();
        return rc;
    }
}
