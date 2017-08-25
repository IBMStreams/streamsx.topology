package com.ibm.streamsx.rest;

import java.io.IOException;

public class StreamingAnalyticsConnectionV2 extends AbstractStreamingAnalyticsConnection {

    static final String USER_NAME = "Bearer";

    private long authExpiryTime;

    StreamingAnalyticsConnectionV2(String authToken, long authExpiryTime,
            String resourcesUrl, String jobsUrl) throws IOException {
        super(USER_NAME, authToken, resourcesUrl, jobsUrl);
        this.authExpiryTime = authExpiryTime;
    }

    /**
     * Get the current API key. In the base class, this is set and never changes
     * but with IAM authentication we need to reauthenticate if it is later than
     * the expiry time of our current key.
     * @return The saved API key
     */
    protected String getApiKey() {
        if (System.currentTimeMillis() > authExpiryTime) {
            refreshApiKey();
        }
        return super.getApiKey();
    }

    private void refreshApiKey() {
        // FIXME Make IAM call to get new token and timeout
        String token = "TODO";
        setApiKey(USER_NAME, token);
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
        return delete(jobsUrl + "/" + jobId);
    }

}
