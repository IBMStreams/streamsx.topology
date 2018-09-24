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

    private String jobsUrl;

    StreamingAnalyticsConnectionV2(
    		StreamingAnalyticsServiceV2 service,
            String resourcesUrl, boolean allowInsecure)
            {
        super(service, resourcesUrl, allowInsecure);
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
            String restUrl = jstring(credentials(), "v2_rest_url");
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

    static StreamingAnalyticsConnectionV2 of(
    		StreamingAnalyticsServiceV2 actualService,
    		JsonObject service,
            boolean allowInsecure)
            throws IOException {
        JsonObject credentials = service.get("credentials").getAsJsonObject();
        String sasResourcesUrl = StreamsRestUtils.getRequiredMember(credentials,
                StreamsRestUtils.MEMBER_V2_REST_URL);
        JsonObject sasResources = StreamsRestUtils.getServiceResources(actualService.getAuthorization(), sasResourcesUrl);
        String instanceUrl = StreamsRestUtils.getRequiredMember(sasResources,
                "streams_self");
        // Find root URL. V2 starts at the instance, we want resources
        String baseUrl = instanceUrl.substring(0, instanceUrl.lastIndexOf("/instances/"));
        String streamsResourcesUrl = StreamsRestUtils.fixStreamsRestUrl(baseUrl);

        StreamingAnalyticsConnectionV2 connection =
                new StreamingAnalyticsConnectionV2(actualService, 
                        streamsResourcesUrl,
                        allowInsecure);
        connection.baseConsoleURL = StreamsRestUtils.getRequiredMember(sasResources, "streams_console");
        return connection;
    }

}
