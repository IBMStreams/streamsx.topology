/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */

package com.ibm.streamsx.rest;

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
import com.ibm.streamsx.rest.StreamsRestUtils.StreamingAnalyticsServiceVersion;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

/**
 * Common code for implementations of {@link IStreamingAnalyticsConnection}
 */
abstract class AbstractStreamingAnalyticsConnection 
        extends AbstractStreamsConnection {

    private static final Logger traceLog = Logger.getLogger("com.ibm.streamsx.rest.AbstractStreamingAnalyticsConnection");

    JsonObject credentials;
    private Instance instance;
    String baseConsoleURL;

    AbstractStreamingAnalyticsConnection(String authorization,
            String resourcesUrl, JsonObject credentials, boolean allowInsecure)
            throws IOException {
        super(authorization, resourcesUrl, allowInsecure);
        this.credentials = credentials;
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
        if (instance != null)
            return instance;
        List<Instance> instances = getInstances();
        if (instances.size() == 1) {
            // Should find one only
            instance = instances.get(0);
            instance.setApplicationConsoleURL(baseConsoleURL);
            return instance;
        } else {
            throw new RESTException("Unexpected number of instances: " + instances.size());
        }
    }

    protected boolean delete(String deleteJobUrl) throws IOException {
        boolean rc = false;
        String sReturn = "";

        Request request = Request
                .Delete(deleteJobUrl)
                .addHeader(AUTH.WWW_AUTH_RESP, getAuthorization())
                .useExpectContinue();

        Response response = executor.execute(request);
        HttpResponse hResponse = response.returnResponse();
        int rcResponse = hResponse.getStatusLine().getStatusCode();

        if (HttpStatus.SC_OK == rcResponse) {
            sReturn = EntityUtils.toString(hResponse.getEntity());
            rc = true;
        } else {
            rc = false;
        }
        traceLog.finest("Request: [" + deleteJobUrl + "]");
        traceLog.finest(rcResponse + ": " + sReturn);
        return rc;
    }

    static AbstractStreamingAnalyticsConnection of(JsonObject config,
            boolean allowInsecure) throws IOException {

        // Get the VCAP service based on the config, and extract credentials
        JsonObject service = VcapServices.getVCAPService(config);

        JsonObject credentials = service.get("credentials").getAsJsonObject();

        StreamingAnalyticsServiceVersion version = 
                StreamsRestUtils.getStreamingAnalyticsServiceVersion(credentials);
        switch (version) {
        case V1:
            return StreamingAnalyticsConnectionV1.of(service, allowInsecure);
        case V2:
        {
            // V2: authorization is constructed with IAM and must be renewed
            String tokenUrl = StreamsRestUtils.getTokenUrl(credentials);
            String apiKey = StreamsRestUtils.getServiceApiKey(credentials);
            JsonObject response = StreamsRestUtils.getTokenResponse(tokenUrl, apiKey);
            String accessToken = StreamsRestUtils.getToken(response);
            if (null != accessToken) {
                String authorization = StreamsRestUtils.createBearerAuth(accessToken);
                long authExpiryMillis = StreamsRestUtils.getTokenExpiryMillis(response);
                return StreamingAnalyticsConnectionV2.of(service, authorization,
                        authExpiryMillis, allowInsecure);
            }
            throw new IllegalStateException("Unable to authenticate Streaming Analytics Service");
        }
        default:
            throw new IllegalStateException("Unknown Streaming Analytics Service version");
        }
    }

}
