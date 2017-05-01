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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

public class StreamingAnalyticsConnection extends StreamsConnection {

    static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.topology.rest.StreamingAnalyticsConnection");

    private String jobsPath;

    /**
     * Basic connection to the Streaming Analytics Instance
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

        String restURL = "";
        StreamingAnalyticsConnection streamingConnection = new StreamingAnalyticsConnection(userId, authToken, restURL);

        String sResources = streamingConnection.getResponseString(sURL);
        if (!sResources.equals("")) {
            JsonParser jParse = new JsonParser();
            JsonObject resources = jParse.parse(sResources).getAsJsonObject();

            restURL = resources.get("streams_rest_url").getAsString();
        }

        if (restURL.equals("")) {
            throw new IllegalStateException("Missing restURL for service");
        }

        streamingConnection.setURL(restURL);
        String[] rTokens = resourcesPath.split("/");
        if (rTokens[3].equals("service_instances")) {
            streamingConnection.setInstanceId(rTokens[4]);
        } else {
            throw new IllegalStateException("Resource Path decoding error.");
        }
        return streamingConnection;
    }


    /**
     * main currently exists to test this object
     * 
     * @param credentials
     *            String representing the VCAP_SERVICES, or a file location
     * @param serviceName
     *            String representing the service name
     */
    public static void main(String[] args) {
        String credentials = args[0];
        String serviceName = args[1];

        System.out.println(credentials);
        System.out.println(serviceName);

        try {
            StreamsConnection sClient = StreamingAnalyticsConnection.createInstance(credentials, serviceName);

            System.out.println("Returning instance");
            Instance instance = sClient.getInstance();

            System.out.println("Returning jobs");
            List<Job> jobs = instance.getJobs();
            for (Job job : jobs) {
                System.out.println("Looking at job: " + job.getId());
                List<Operator> operators = job.getOperators();
                for (Operator op : operators) {
                    System.out.println("Looking at metrics for job");
                    List<Metric> metrics = op.getMetrics();
                }
            }

            if (!jobs.isEmpty()) {
                System.out.println("Getting first job");
                Job job = jobs.get(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
