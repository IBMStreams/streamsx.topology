/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.net.ssl.SSLHandshakeException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
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

import com.google.gson.Gson;
import com.ibm.streamsx.rest.primitives.Instance;
import com.ibm.streamsx.rest.primitives.InstancesArray;
import com.ibm.streamsx.rest.primitives.Job;
import com.ibm.streamsx.rest.primitives.Metric;
import com.ibm.streamsx.rest.primitives.Operator;

public class StreamsConnection {

    private String url;
    private String instanceId;
    private String apiKey;
    private boolean allowInsecureHosts;

    private Executor executor;

    private final Gson gson = new Gson();

    /**
     * sets the url for this object removing the trailing slash
     * 
     * @param url
     *            String root path to the REST API
     */
    private void setURL(String url) {
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
    public StreamsConnection(String userName, String authToken, String url) {
        String apiCredentials = userName + ":" + authToken;
        apiKey = "Basic " + DatatypeConverter.printBase64Binary(apiCredentials.getBytes(StandardCharsets.UTF_8));

        executor = Executor.newInstance();
        setURL(url);
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
    public String getResponseString(String inputString) throws IOException {
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
        // FIXME: remove these lines
        System.out.println("-----------------");
        System.out.println(inputString);
        System.out.println(rcResponse + ": " + sReturn);
        System.out.println("-----------------");
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
        StreamsConnection sClient = new StreamsConnection(userName, authToken, url);

        try {
          System.out.println("Returning instances");
          List<Instance> instances = sClient.getInstances();

          for (Instance instance : instances) {
              System.out.println("Returning jobs");
              List<Job> jobs = instance.getJobs();

              System.out.println("Returning operators");
              for (Job job : jobs) {
                  System.out.println("Looking at job");
                  List<Operator> operators = job.getOperators();
                  for (Operator op : operators) {
                      System.out.println("Looking at metrics for job");
                      List<Metric> metrics = op.getMetrics();
                  }
              }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
    }
}
