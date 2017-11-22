/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.rest;

import static com.ibm.streamsx.rest.StreamsRestUtils.MEMBER_PASSWORD;
import static com.ibm.streamsx.rest.StreamsRestUtils.MEMBER_USERID;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;

import java.io.IOException;

import org.apache.http.client.fluent.Executor;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.StreamsRestUtils.StreamingAnalyticsServiceVersion;
import com.ibm.streamsx.topology.context.AnalyticsServiceProperties;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

public class StreamsRestFactory {

    private StreamsRestFactory() {}

    /**
     * Connect to IBM Streams REST API instance directly (eg. a DISTRIBUTED
     * instance).
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
    public static IStreamsConnection createStreamsConnection(String userName,
            String authToken, String resourcesUrl)
                    throws IOException {
        StreamsConnectionImpl connection = new StreamsConnectionImpl(userName,
                StreamsRestUtils.createBasicAuth(userName, authToken),
                resourcesUrl, false);
        connection.init();
        return connection;
    }

    /**
     * Connect to IBM Streams REST API instance directly (eg. a DISTRIBUTED
     * instance), possibly without requiring validation of host TLS/SSL
     * connection certificates. This <strong>not</strong> recommended in
     * production environments.
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
    public static IStreamsConnection createStreamsConnection(String userName,
            String authToken, String resourcesUrl, boolean allowInsecure)
                    throws IOException {
        StreamsConnectionImpl connection = new StreamsConnectionImpl(userName,
                StreamsRestUtils.createBasicAuth(userName, authToken),
                resourcesUrl, allowInsecure);
        connection.init();
        return connection;
    }

    /**
     * Connect to IBM Streaming Analytics Service instance's Streams REST API
     * given a VCAP file and service name.
     *
     * @param vcap
     *            Path to VCAP file or representation of VCAP in JSON
     * @param serviceName
     *            Name of the service in vcap
     * @return a connection to IBM Streaming Analytics service
     * @throws IOException
     */
    public static IStreamingAnalyticsConnection createStreamingAnalyticsConnection(
            String vcap, String serviceName) throws IOException {
        return createStreamingAnalyticsConnection(vcap, serviceName, false);
    }

    /**
     * Connect to IBM Streaming Analytics Service instance's Streams REST API
     * given a VCAP file and service name.
     *
     * @param vcap
     *            Path to VCAP file or representation of VCAP in JSON
     * @param serviceName
     *            Name of the service in vcap
     * @param allowInsecure
     *            Flag to allow insecure TLS/SSL connections. This is
     *            <strong>not</strong> recommended in a production environment.
     * @return a connection to IBM Streaming Analytics service
     * @throws IOException
     */
    public static IStreamingAnalyticsConnection createStreamingAnalyticsConnection(
            String vcap, String serviceName, boolean allowInsecure)
            throws IOException {

        JsonObject config = new JsonObject();

        config.addProperty(SERVICE_NAME, serviceName);
        config.addProperty(VCAP_SERVICES, vcap);

        return StreamsRestFactory.createStreamingAnalyticsConnection(config,
                allowInsecure);
    }

    /**
     * Connect to IBM Streaming Analytics Service instance's Streams REST API
     * given a configuration.
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
    public static IStreamingAnalyticsConnection createStreamingAnalyticsConnection(
            JsonObject config) throws IOException {
        return createStreamingAnalyticsConnection(config, false);
    }

    /**
     * Connect to IBM Streaming Analytics Service instance's Streams REST API
     * given a configuration.
     * <p>
     * The contents of the VCAP in config determine the version of the
     * Streaming Analytics Service. Version 1 expects "userid" and "password".
     * Version 2 will use another member name (TBD)
     * @param config
     *            JSON configuration, must contain at least
     *            {@link AnalyticsServiceProperties.VCAP_SERVICES} and
     *            {@link AnalyticsServiceProperties.SERVICE_NAME} members
     * @param allowInsecure
     *            Flag to allow insecure TLS/SSL connections. This is
     *            <strong>not</strong> recommended in a production environment.
     * @return a connection to IBM Streaming Analytics service
     * @throws IOException
     */
    public static IStreamingAnalyticsConnection createStreamingAnalyticsConnection(
            JsonObject config, boolean allowInsecure) throws IOException {

        // Get the VCAP service based on the config, and extract credentials
        JsonObject service = VcapServices.getVCAPService(config);

        JsonObject credentials = service.get("credentials").getAsJsonObject();

        // Since version and authentication are currently tied, we implement
        // together, but they could be separated out, eg. into authentication
        // strategy classes used by the connection implementations.
        StreamingAnalyticsServiceVersion version = 
                StreamsRestUtils.getStreamingAnalyticsServiceVersion(credentials);
        switch (version) {
        case V1:
        {
            // V1: can construct authorization directly and use indefinitely
            String userId = StreamsRestUtils.getRequiredMember(credentials, MEMBER_USERID);
            String authToken = StreamsRestUtils.getRequiredMember(credentials, MEMBER_PASSWORD);
            String authorization = StreamsRestUtils.createBasicAuth(userId, authToken);
            String restUrl = StreamsRestUtils.getRequiredMember(credentials, "rest_url");
            String sasResourcesUrl = restUrl + StreamsRestUtils.getRequiredMember(credentials, "resources_path");
            JsonObject sasResources = getServiceResources(authorization, sasResourcesUrl);
            String streamsRestUrl = StreamsRestUtils.getRequiredMember(sasResources, "streams_rest_url");
            // In V1, streams_rest_url is missing /resources
            String streamsResoursesUrl = fixStreamsRestUrl(streamsRestUrl);

            StreamingAnalyticsConnectionV1 connection =
                    new StreamingAnalyticsConnectionV1(userId, authToken,
                    streamsResoursesUrl, credentials, allowInsecure);
            connection.init();
            return connection;
        }
        case V2:
        {
            // V2: authorization is constructed with IAM and must be renewed
            String tokenUrl = StreamsRestUtils.getTokenUrl(credentials);
            String apiKey = StreamsRestUtils.getServiceApiKey(credentials);
            JsonObject response = StreamsRestUtils.getTokenResponse(tokenUrl, apiKey);
            String accessToken = StreamsRestUtils.getToken(response);
            if (null != accessToken) {
                String authorization = StreamsRestUtils.createBearerAuth(accessToken);
                String sasResourcesUrl = StreamsRestUtils.getRequiredMember(credentials,
                        StreamsRestUtils.MEMBER_V2_REST_URL);
                JsonObject sasResources = getServiceResources(authorization, sasResourcesUrl);
                String instanceUrl = StreamsRestUtils.getRequiredMember(sasResources,
                        "streams_self");
                // Find root URL. V2 starts at the instance, we want resources
                String baseUrl = instanceUrl.substring(0, instanceUrl.lastIndexOf("/instances/"));
                String streamsResoursesUrl = fixStreamsRestUrl(baseUrl);

                long authExpiryMillis = StreamsRestUtils.getTokenExpiryMillis(response);
                StreamingAnalyticsConnectionV2 connection =
                        new StreamingAnalyticsConnectionV2(authorization,
                        authExpiryMillis, streamsResoursesUrl, credentials,
                        allowInsecure);
                connection.init();
                return connection;
            }
            throw new IllegalStateException("Unable to authenticate Streaming Analytics Service");
        }
        default:
            throw new IllegalStateException("Unknown Streaming Analytics Service version");
        }
    }

    private static String fixStreamsRestUrl(String streamsRestUrl) {
        final String suffix = "resources";
        StringBuilder sb = new StringBuilder(streamsRestUrl.length() + 1 + suffix.length());
        sb.append(streamsRestUrl);
        if (!streamsRestUrl.endsWith("/")) {
            sb.append('/');
        }
        sb.append(suffix);
        String streamsResourcesUrl = sb.toString();
        return streamsResourcesUrl;
    }

    private static JsonObject getServiceResources(String authorization,
            String url)throws IOException {
        JsonObject resources = StreamsRestUtils.getGsonResponse(Executor.newInstance(),
                authorization, url);
        if (null == resources) {
            throw new IllegalStateException("Missing resources for service");
        }
        return resources;
    }
}
