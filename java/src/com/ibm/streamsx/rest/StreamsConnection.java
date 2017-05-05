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

public class StreamsConnection {

    static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.topology.rest.StreamsConnection");

    private final String userName;
    private String url;
    private String instanceId;
    protected String apiKey;
    private boolean allowInsecureHosts = false;

    protected Executor executor;

    /**
     * sets the url for this object removing the trailing slash
     * 
     * @param url
     *            String root path to the REST API
     */
    protected void setURL(String url) {
        if (url.equals("") || (url.charAt(url.length() - 1) != '/')) {
            this.url = url;
        } else {
            this.url = url.substring(0, url.length() - 1);
        }
    }

    /**
     * Basic connection to a streams instance
     * 
     * @param userName
     *            String representing the userName to connect to the instance
     * @param authToken
     *            String representing the password to connect to the instance
     * @param url
     *            String representing the root url to the REST API
     */
    protected StreamsConnection(String userName, String authToken, String url) {
        this.userName = userName ;
        String apiCredentials = userName + ":" + authToken;
        apiKey = "Basic " + DatatypeConverter.printBase64Binary(apiCredentials.getBytes(StandardCharsets.UTF_8));

        executor = Executor.newInstance();
        setURL(url);
    }

    /**
     * Basic connection to a streams instance
     * 
     * @param userName
     *            String representing the userName to connect to the instance
     * @param authToken
     *            String representing the password to connect to the instance
     * @param url
     *            String representing the root url to the REST API
     */
    public static StreamsConnection createInstance(String userName, String authToken, String url) {
        return new StreamsConnection(userName, authToken, url);
    }

    /**
     * @param url
     * @return old value of the url
     */
    public String setStreamsInstanceRestURL(String url) {
        String old_url = url;
        setURL(url);
        return old_url;
    }

    /**
     * @param id
     * @return id that has been set
     */
    public String setInstanceId(String id) {
        instanceId = id;
        return id;
    }

    /**
     * @param inputString
     *            Rest call to make
     * @return Response from the inputString
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
        } else {
            String httpError = "HttpStatus is " + rcResponse + " for url " + inputString;
            throw new IllegalStateException(httpError);
        }
        traceLog.finest("Request: " + inputString);
        traceLog.finest(rcResponse + ": " + sReturn);
        return sReturn;
    }

    /**
     * @return List of {@Instance}
     * @throws IOException
     */
    public List<Instance> getInstances() throws IOException {
        String instancesURL = url + "/instances/";

        String sReturn = getResponseString(instancesURL);
        InstancesArray iArray = new InstancesArray(this, sReturn);

        return iArray.getInstances();
    }

    /**
     * @return {@Instance}
     * @throws IOException
     */
    public Instance getInstance() throws IOException {
        Instance si = null;
        if (instanceId.equals("")) {
            // should add some fallback code to see if there's only one instance
            throw new IllegalArgumentException("Missing instance name");
        } else {
            String instanceURL = url + "/instances/" + instanceId;
            String sReturn = getResponseString(instanceURL);

            si = new Instance(this, sReturn);
        }
        return si;
    }

    /**
     * @param id
     * @return {@Instance}
     * @throws IOException
     */
    public Instance getInstance(String id) throws IOException {
        instanceId = id;
        return getInstance();
    }

    /**
     * @param allowInsecure
     *            boolean whether insecure hosts are allowed(true) or not(false)
     * @return true if insecure hosts will be allowed false if insecure hosts
     *         will not be allowed
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
        traceLog.info("Insecure Host Connection enabled");
        return allowInsecureHosts;
    }

    /**
     * @param jobId
     *            string identifying the job to be cancelled
     * @return true if job is cancelled
     * @throws Exception
     */
    public boolean cancelJob(String jobId) throws Exception {
        boolean rc = true;
        InvokeCancel cancelJob = new InvokeCancel(new BigInteger(jobId), userName );
        cancelJob.invoke();
        return rc;
    }

    /**
     * Main function to test this class for now will be removed eventually
     */
    public static void main(String[] args) {
        String userName = args[0];
        String authToken = args[1];
        String url = args[2];
        String instanceName = args[3];

        System.out.println(userName);
        System.out.println(authToken);
        System.out.println(url);
        System.out.println(instanceName);
        StreamsConnection sClient = StreamsConnection.createInstance(userName, authToken, url);

        if (args.length == 5) {
            if (args[4].equals("true")) {
                sClient.allowInsecureHosts(true);
            }
        }

        try {
            System.out.println("Instance: ");
            List<Instance> instances = sClient.getInstances();

            for (Instance instance : instances) {
                List<Job> jobs = instance.getJobs();
                for (Job job : jobs) {
                    System.out.println("Job: " + job.toString());
                    List<Operator> operators = job.getOperators();
                    for (Operator op : operators) {
                        System.out.println("Operator: " + op.toString());
                        List<Metric> metrics = op.getMetrics();
                        for (Metric m : metrics) {
                            System.out.println("Metric: " + m.toString());
                        }
                        List<OutputPort> outP = op.getOutputPorts();
                        for (OutputPort oport : outP) {
                            System.out.println("Output Port: " + oport.toString());
                            for (Metric om : oport.getMetrics()) {
                                System.out.println("Output Port Metric: " + om.toString());
                            }
                        }
                        List<InputPort> inP = op.getInputPorts();
                        for (InputPort ip : inP) {
                            System.out.println("Input Port: " + ip.toString());
                            for (Metric im : ip.getMetrics()) {
                                System.out.println("Input Port Metric: " + im.toString());
                            }
                        }
                    }
                }

                if (!jobs.isEmpty()) {
                    System.out.println("Removing first job specifically");
                    Job job = jobs.get(0);
                    if (job.cancel()) {
                        System.out.println("Job canceled");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
