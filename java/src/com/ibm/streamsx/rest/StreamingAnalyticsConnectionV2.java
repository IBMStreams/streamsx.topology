/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class StreamingAnalyticsConnectionV2 extends AbstractStreamingAnalyticsConnection {

    private static final String MEMBER_EXPIRATION = "expiration";
    private static final String MEMBER_ACCESS_TOKEN = "access_token";
    private static final long MS = 1000L;
    private static final long EXPIRY_PAD_MS = 300 * MS;

    private long authExpiryTime;
    private String jobsUrl;

    StreamingAnalyticsConnectionV2(String authorization, long authExpiryTime,
            String resourcesUrl, JsonObject credentials) throws IOException {
        super(authorization, resourcesUrl, credentials);
        this.authExpiryTime = authExpiryTime;
    }

    // Synchronized because it needs to read and possibly write two members
    // that are interdependent: authExpiryTime and authorization. Should be
    // fast enough without getting tricky: contention should be rare because
    // of the way we use this, and this should be fast compared to the network
    // I/O that typically follows using the returned authorization.
    @Override
    synchronized protected String getAuthorization() {
        if (System.currentTimeMillis() > authExpiryTime) {
            refreshAuthorization();
        }
        return authorization;
    }

    private void refreshAuthorization() {
        String tokenUrl = StreamsRestUtils.getTokenUrl(credentials);
        String apiKey = StreamsRestUtils.getServiceApiKey(credentials);
        JsonObject response = StreamsRestUtils.getToken(tokenUrl, apiKey);
        if (null != response && response.has(MEMBER_ACCESS_TOKEN)
                && response.has(MEMBER_EXPIRATION)) {
            String accessToken = response.get(MEMBER_ACCESS_TOKEN).getAsString();
            setAuthorization(StreamsRestUtils.createBearerAuth(accessToken));
            long expirySecs = response.get(MEMBER_EXPIRATION).getAsLong();
            authExpiryTime = (expirySecs * MS) - EXPIRY_PAD_MS;
        }
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
    public boolean cancelJob(String jobId) throws IOException {
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
            }
        }
        return delete(jobsUrl + "/" + jobId);
    }

}
