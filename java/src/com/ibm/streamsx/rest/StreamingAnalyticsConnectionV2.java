/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

class StreamingAnalyticsConnectionV2 extends AbstractStreamingAnalyticsConnection {

    private long authExpiryTime;
    private String jobsUrl;
    private final String tokenUrl;
    private final String apiKey;

    StreamingAnalyticsConnectionV2(String authorization, long authExpiryTime,
            String resourcesUrl, JsonObject credentials, boolean allowInsecure)
            throws IOException {
        super(authorization, resourcesUrl, credentials, allowInsecure);
        this.authExpiryTime = authExpiryTime;
        this.tokenUrl = StreamsRestUtils.getTokenUrl(credentials);
        this.apiKey = StreamsRestUtils.getServiceApiKey(credentials);
    }

    // Synchronized because it needs to read and possibly write two members
    // that are interdependent: authExpiryTime and authorization. Should be
    // fast enough without getting tricky: contention should be rare because
    // of the way we use this, and this should be fast compared to the network
    // I/O that typically follows using the returned authorization.
    @Override
    synchronized String getAuthorization() {
        if (authorization == null ||
                System.currentTimeMillis() > authExpiryTime) {
            refreshAuthorization();
        }
        return authorization;
    }

    private void refreshAuthorization() {
        JsonObject response = StreamsRestUtils.getTokenResponse(tokenUrl, apiKey);
        if (null != response) {
            String accessToken = StreamsRestUtils.getToken(response);
            if (null != accessToken) {
                setAuthorization(StreamsRestUtils.createBearerAuth(accessToken));
                authExpiryTime = StreamsRestUtils.getTokenExpiryMillis(response);
            }
        }
    }

    /**
     * Cancels a job that has been submitted to IBM Streaming Analytics service
     *
     * @param instanceId
     *            string indicating the instance id of the job
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
        if (null == jobsUrl) {
            String restUrl = jstring(credentials, "v2_rest_url");
            JsonObject response = StreamsRestUtils.getGsonResponse(executor,
                    getAuthorization(), restUrl);
            JsonElement element = response.get("jobs");
            if (null != element) {
                jobsUrl = element.getAsString();
            } else {
                throw new RESTException("Unable to get jobs URL");
            }
        }

        return delete(jobsUrl + "/" + jobId);
    }

    static StreamingAnalyticsConnectionV2 of(JsonObject service,
            String authorization, long authExpiryMillis, boolean allowInsecure)
            throws IOException {
        JsonObject credentials = service.get("credentials").getAsJsonObject();
        String sasResourcesUrl = StreamsRestUtils.getRequiredMember(credentials,
                StreamsRestUtils.MEMBER_V2_REST_URL);
        JsonObject sasResources = StreamsRestUtils.getServiceResources(authorization, sasResourcesUrl);
        String instanceUrl = StreamsRestUtils.getRequiredMember(sasResources,
                "streams_self");
        // Find root URL. V2 starts at the instance, we want resources
        String baseUrl = instanceUrl.substring(0, instanceUrl.lastIndexOf("/instances/"));
        String streamsResourcesUrl = StreamsRestUtils.fixStreamsRestUrl(baseUrl);

        StreamingAnalyticsConnectionV2 connection =
                new StreamingAnalyticsConnectionV2(authorization,
                        authExpiryMillis, streamsResourcesUrl, credentials,
                        allowInsecure);
        connection.baseConsoleURL = StreamsRestUtils.getRequiredMember(sasResources, "streams_console");
        connection.init();
        return connection;
    }

}
