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
    boolean cancelJob(String instanceId, String jobId) throws IOException {
        if (null == jobsUrl) {
            String restUrl = jstring(credentials, "rest_url");
            String resourcesPath = jstring(credentials, "resources_path");
            StringBuilder sb = new StringBuilder(restUrl.length() + resourcesPath.length());
            sb.append(restUrl);
            sb.append(resourcesPath);
            String statusUrl = sb.toString();
            JsonObject response = StreamsRestUtils.getGsonResponse(executor,
                    getAuthorization(), statusUrl);
            JsonElement element = response.get("jobs");
            if (null != element) {
                jobsUrl = element.getAsString();
            } else {
                throw new RESTException("Unable to get jobs URL");
            }
        }
        Instance instance = getInstance();
        if (!instance.getId().equals(instanceId)) {
            // Sanity check, should not happen
            throw new RESTException("Unable to cancel job in instance " + instanceId);
        }

        return delete(jobsUrl + "/" + jobId);
    }

}
