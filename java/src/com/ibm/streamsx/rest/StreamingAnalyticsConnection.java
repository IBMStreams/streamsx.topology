/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

/**
 * Connection to a Streaming Analytics Instance
 *
 */
public class StreamingAnalyticsConnection extends StreamsConnection {

    static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.rest.StreamingAnalyticsConnection");

    private String jobsPath;
    private String instanceId;

    /**
     * Connection to IBM Streaming Analytics service
     *
     * @param credentialsFile
     *            Credentials from the Streaming Analytics File
     * @param serviceName
     *            Name of the service in the file above
     * @throws IOException
     */
    private StreamingAnalyticsConnection(String userName, String authToken, String url) {
        super(userName, authToken, url);
    }

    /**
     * Connection to IBM Streaming Analytics service
     *
     * @param credentialsFile
     *            Credentials from the Streaming Analytics File
     * @param serviceName
     *            Name of the service in the file above
     * @return a connection to IBM Streaming Analytics service
     * @throws IOException
     */
    public static StreamingAnalyticsConnection createInstance(String credentialsFile, String serviceName)
            throws IOException {

        JsonObject streamingAnalyticsCredentials = new JsonObject();

        streamingAnalyticsCredentials.addProperty(SERVICE_NAME, serviceName);
        streamingAnalyticsCredentials.addProperty(VCAP_SERVICES, credentialsFile);

        JsonObject service = VcapServices.getVCAPService(streamingAnalyticsCredentials);

        JsonObject credential = new JsonObject();
        credential = service.get("credentials").getAsJsonObject();

        String userId = credential.get("userid").getAsString();
        String authToken = credential.get("password").getAsString();

        String resourcesPath = credential.get("resources_path").getAsString();
        String sURL = credential.get("rest_url").getAsString() + resourcesPath;

        String jobPath = credential.get("jobs_path").getAsString();
        String jobString = credential.get("rest_url").getAsString() + jobPath;

        String restURL = "";
        StreamingAnalyticsConnection streamingConnection = new StreamingAnalyticsConnection(userId, authToken, restURL);
        streamingConnection.setJobsPath(jobString);

        String sResources = streamingConnection.getResponseString(sURL);
        if (!sResources.equals("")) {
            JsonParser jParse = new JsonParser();
            JsonObject resources = jParse.parse(sResources).getAsJsonObject();

            restURL = resources.get("streams_rest_url").getAsString();
        }

        if (restURL.equals("")) {
            throw new IllegalStateException("Missing restURL for service");
        }

        streamingConnection.setStreamsRESTURL(restURL);
        String[] rTokens = resourcesPath.split("/");
        if (rTokens[3].equals("service_instances")) {
            streamingConnection.setInstanceId(rTokens[4]);
        } else {
            throw new IllegalStateException("Resource Path decoding error.");
        }
        return streamingConnection;
    }

    private void setInstanceId(String id) {
        instanceId = id;
    }

    private void setJobsPath(String sJobs) {
        jobsPath = sJobs;
    }

    /**
     * Gets the {@link Instance IBM Streams Instance} object for the Streaming
     * Analytics service.
     *
     * @return an {@link Instance IBM Streams Instance} associated with this
     *         connection
     *
     * @throws IOException
     */
    public Instance getInstance() throws IOException {
        return super.getInstance(instanceId);
    }

    /**
     * Cancels a job that has been submitted to IBM Streaming Analytcis service
     *
     * @param jobId
     *            string indicating the job id to be canceled
     * @return boolean indicating
     *         <ul>
     *         <li>true - if job is cancelled</li>
     *         <li>false - if the job still exists</li>
     *         </ul>
     * @throws IOException
     */
    public boolean cancelJob(String jobId) throws IOException {
        boolean rc = false;
        String sReturn = "";
        String deleteJob = jobsPath + "?job_id=" + jobId;

        Request request = Request.Delete(deleteJob).addHeader(AUTH.WWW_AUTH_RESP, apiKey).useExpectContinue();

        Response response = executor.execute(request);
        HttpResponse hResponse = response.returnResponse();
        int rcResponse = hResponse.getStatusLine().getStatusCode();

        if (HttpStatus.SC_OK == rcResponse) {
            sReturn = EntityUtils.toString(hResponse.getEntity());
            rc = true;
        } else {
            rc = false;
        }
        traceLog.finest("Request: [" + deleteJob + "]");
        traceLog.finest(rcResponse + ": " + sReturn);
        return rc;
    }

}
