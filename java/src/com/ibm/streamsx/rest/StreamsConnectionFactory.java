/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;

import java.io.IOException;

import org.apache.http.client.fluent.Executor;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ibm.streamsx.rest.StreamsConnectionUtils.IAMAuth;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

public class StreamsConnectionFactory {

    private StreamsConnectionFactory() {}

    /**
     * Connect to IBM Streams instance directly (eg. a DISTRIBUTED instance).
     * 
     * @param userName
     *            The userName used to connect to the instance
     * @param authToken
     *            The password used to connect to the instance
     * @param resourcesUrl
     *            The root url to the REST API resources, for example:
     *            https://server:port/streams/rest/resources
     * @return a connection to IBM Streams
     * @throws IOException
     */
    public static StreamsConnection createStreamsConnection(String userName,
            String authToken, String resourcesUrl)
                    throws IOException {
        return new StreamsConnectionImpl(userName, authToken, resourcesUrl, false);
    }

    /**
     * Connect to IBM Streams instance directly (eg. a DISTRIBUTED instance),
     * possibly without requiring validation of host TLS/SSL connection
     * certificates. This <strong>not</strong> recommended in production
     * environments.
     * 
     * @param userName
     *            The userName used to connect to the instance
     * @param authToken
     *            The password used to connect to the instance
     * @param resourcesUrl
     *            The root url to the REST API resources, for example:
     *            https://server:port/streams/rest/resources
     * @param allowInsecure
     *            Flag to allow insecure TLS/SSL connections. This is
     *            <strong>not</strong> recommended in a production environment.
     * @return a connection to IBM Streams
     * @throws IOException
     */
    public static StreamsConnection createStreamsConnection(String userName,
            String authToken, String resourcesUrl, boolean allowInsecure)
                    throws IOException {
        return new StreamsConnectionImpl(userName, authToken, resourcesUrl, allowInsecure);
    }

    /**
     * Connect to IBM Streaming Analytics Service instance given a VCAP file
     * and service name.
     *
     * @param vcap
     *            Path to VCAP file or representation of VCAP in JSON
     * @param serviceName
     *            Name of the service in vcap
     * @return a connection to IBM Streaming Analytics service
     * @throws IOException
     */
    public static StreamingAnalyticsConnection createStreamingAnalyticsConnection(
            String vcap, String serviceName) throws IOException {

        JsonObject config = new JsonObject();

        config.addProperty(SERVICE_NAME, serviceName);
        config.addProperty(VCAP_SERVICES, vcap);

        return StreamsConnectionFactory.createStreamingAnalyticsConnection(config);
    }

    /**
     * Connect to IBM Streaming Analytics Service instance given a configuration.
     * <p>
     * The contents of the VCAP in config determine the version of the
     * Streaming Analytics Service. Version 1 expects "userid" and "password".
     * Version 2 will use another member name (TBD)
     * @param config
     *            JSON configuration, must contain at least
     *            {@link AnalyticsServiceProperties.VCAP_SERVICES} and
     *            {@link AnalyticsServiceProperties.SERVICE_NAME} members
     * @return a connection to IBM Streaming Analytics service
     * @throws IOException
     */
    public static StreamingAnalyticsConnection createStreamingAnalyticsConnection(
            JsonObject config) throws IOException {

        // Get the VCAP service based on the config, and extract credentials
        JsonObject service = VcapServices.getVCAPService(config);

        JsonObject credentials = service.get("credentials").getAsJsonObject();

        // Pull the required pieces out of the credentials (will throw if any
        // are missing)
        String restUrl = StreamsConnectionUtils.getRequiredMember(credentials, "rest_url");
        String sasResourcesUrl = restUrl + StreamsConnectionUtils.getRequiredMember(credentials, "resources_path");
        String sasJobsUrl = restUrl + StreamsConnectionUtils.getRequiredMember(credentials, "jobs_path");

        // Need to figure out service version based on contents. Ideally, it
        // would have a version indicator that would allow us to directly
        // to an appropriate factory method, but that is not yet in place.
        // Also, strictly speaking the authentication scheme and the REST API
        // are not connected, but in practice they are for now, so it's not
        // worth separating yet.
        if (credentials.has("userid") && credentials.has("password")) {
            // V1: can construct apiKey directly and use indefinitely
            String userId = StreamsConnectionUtils.getRequiredMember(credentials, "userid");
            String authToken = StreamsConnectionUtils.getRequiredMember(credentials, "password");
            String apiKey = StreamsConnectionUtils.createApiKey(userId, authToken);
            String streamsResoursesUrl = getStreamsResourceUrl(apiKey,
                    sasResourcesUrl);

            return new StreamingAnalyticsConnectionV1(userId, authToken,
                    streamsResoursesUrl, sasJobsUrl);
        } else if (credentials.has("service_id")) { // TODO: correct member name
            // V2: apiKey is constructed with IAM and must be renewed
            // FIXME: Clean up once we know exactly how this works
            IAMAuth iamAuth = StreamsConnectionUtils.getIAMAuth(credentials);
            String apiKey = StreamsConnectionUtils.createApiKey(
                    StreamingAnalyticsConnectionV2.USER_NAME, iamAuth.authToken);
            String streamsResoursesUrl = getStreamsResourceUrl(apiKey,
                    sasResourcesUrl);

            return new StreamingAnalyticsConnectionV2(iamAuth.authToken,
                    iamAuth.authTokenExpiry, streamsResoursesUrl, sasJobsUrl);
        } else {
            throw new IllegalStateException("Unknown Streaming Analytics Service version");
        }
    }

    private static String getStreamsResourceUrl(String apiKey, String sasResourcesUrl)
            throws IOException {
        // Query the resources URL so we can 
        String sasResources = StreamsConnectionUtils.getResponseString(Executor.newInstance(),
                apiKey, sasResourcesUrl);

        if (null == sasResources || sasResources.isEmpty()) {
            throw new IllegalStateException("Missing resources for service");
        }

        JsonParser jParse = new JsonParser();
        JsonObject resources = jParse.parse(sasResources).getAsJsonObject();
        String streamsResoursesUrl = StreamsConnectionUtils.getRequiredMember(resources, "streams_rest_url")
                + "/resources";
        return streamsResoursesUrl;
    }
}
