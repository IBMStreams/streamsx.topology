/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import static com.ibm.streamsx.rest.StreamsRestUtils.MEMBER_PASSWORD;
import static com.ibm.streamsx.rest.StreamsRestUtils.MEMBER_USERID;

import java.io.IOException;

import com.google.gson.JsonObject;

/**
 * Connection to a Streaming Analytics Instance
 *
 */
class StreamingAnalyticsConnectionV1 extends AbstractStreamingAnalyticsConnection {

    /**
     * Connection to IBM Streaming Analytics service
     *
     * @param userName
     *            String representing the userName to connect to the instance
     * @param authToken
     *            String representing the password to connect to the instance
     * @param resourcesUrl
     *            String representing the root url to the REST API resources,
     *            for example: https:server:port/streams/rest/resources
     * @param allowInsecure 
     * @param jobsUrl
     *            String representing the url to the Streaming Analytics
     *            Service jobs REST API.
     * @throws IOException
     */
    StreamingAnalyticsConnectionV1(String userName, String authToken,
            String resourcesUrl, JsonObject credentials, boolean allowInsecure)
            throws IOException {
        super(StreamsRestUtils.createBasicAuth(userName, authToken),
                resourcesUrl, credentials, allowInsecure);
    }

    /**
     * Cancels a job that has been submitted to IBM Streaming Analytics service
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
    @Override
    boolean cancelJob(Instance instance, String jobId) throws IOException {
        String restUrl = StreamsRestUtils.getRequiredMember(credentials, "rest_url");
        String jobsUrl = restUrl + StreamsRestUtils.getRequiredMember(credentials, "jobs_path");
        return delete(jobsUrl + "?job_id=" + jobId);
    }

    @Override
    String getAuthorization() {
        return authorization;
    }

    static StreamingAnalyticsConnectionV1 of(JsonObject service,
            boolean allowInsecure) throws IOException {
        JsonObject credentials = service.get("credentials").getAsJsonObject();
        String userId = StreamsRestUtils.getRequiredMember(credentials, MEMBER_USERID);
        String authToken = StreamsRestUtils.getRequiredMember(credentials, MEMBER_PASSWORD);
        String authorization = StreamsRestUtils.createBasicAuth(userId, authToken);
        String restUrl = StreamsRestUtils.getRequiredMember(credentials, "rest_url");
        String sasResourcesUrl = restUrl + StreamsRestUtils.getRequiredMember(credentials, "resources_path");
        JsonObject sasResources = StreamsRestUtils.getServiceResources(authorization, sasResourcesUrl);
        String streamsBaseUrl = StreamsRestUtils.getRequiredMember(sasResources, "streams_rest_url");
        // In V1, streams_rest_url is missing /resources
        String streamsResourcesUrl = StreamsRestUtils.fixStreamsRestUrl(streamsBaseUrl);

        StreamingAnalyticsConnectionV1 connection =
                new StreamingAnalyticsConnectionV1(userId, authToken,
                streamsResourcesUrl, credentials, allowInsecure);
        connection.baseConsoleURL = StreamsRestUtils.getRequiredMember(sasResources, "streams_console_url");
        connection.init();
        return connection;
    }
}
