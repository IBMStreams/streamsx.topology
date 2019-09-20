/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019 
 */
package com.ibm.streamsx.rest.internal;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Function;

import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.annotations.Expose;
import com.ibm.streamsx.topology.internal.streams.Util;

/**
 * Authenticator which uses the Cloud Pak for Data standalone configuration
 * security service.
 */
public class StandaloneAuthenticator implements Function<Executor,String> {
    private String resourcesUrl;
    private String userName;
    private String password;
    private String securityUrl;
    private String serviceAuth;
    private long expire;    // in ms since epoch

    private JsonObject cfg;

    // Resources response array
    private static final String RESOURCES = "resources";
    // Resource name for security service URL
    private static final String ACCESS_TOKENS_RESOURCE = "accessTokens";
    // Resource name for instances URL
    private static final String BUILDS_RESOURCE = "builds";
    // Resource name for instances URL
    private static final String INSTANCES_RESOURCE = "instances";
    // Instances response array
    private static final String INSTANCES_ARRAY = "instances";
    // Instance URL element
    private static final String SELF = "self";
    // Request / response info for security tokens
    private static final String AUDIENCE_STREAMS = "{\"audience\":[\"streams\"]}";
    private static final String EXPIRE_TIME = "expireTime";
    private static final String ACCESS_TOKEN = "accessToken";
    // Service definition elements
    private static final String SERVICE_TOKEN_EXPIRE = "service_token_expire";
    private static final String CONNECTION_INFO = "connection_info";
    // Connection info elements
    private static final String SERVICE_TOKEN = "service_token";
    private static final String SERVICE_REST_ENDPOINT = "serviceRestEndpoint";
    private static final String SERVICE_BUILD_ENDPOINT = "serviceBuildEndpoint";

    private static long MS = 1000;

    /**
     * Create an authenticator for a Cloud Pak for Data standalone configuration
     * security service from an endpoint URL.
     * 
     * <p>Once created, the caller should use {@link #config(boolean)} to
     * configure the authentication, and {@link #apply} to get the contents
     * for an {@code Authentication} header.
     * 
     * <p>See {@link #config(boolean)} for details on the JSON configuration
     * service definition object.
     * 
     * @param endpoint Resources URL for either Streams rest service or build
     * service.
     * @param userName User name to authenticate as. Defaults to the environment
     * variable {@code STREAMS_USERNAME} or the operating system identifier if not set.
     * @param password Password to authenticate with. Defaults to the environment
     * variable {@code STREAMS_PASSWORD} if not set.
     * 
     * @return An instance of StandaloneAuthenticator.
     */
    public static StandaloneAuthenticator of(String endpoint, String userName, String password) {
        // Fill in user / password defaults if required
        if (userName == null || password == null) {
            String[] values = Util.getDefaultUserPassword(userName, password);
            userName = values[0];
            password = values[1];
        }

        return new StandaloneAuthenticator(endpoint, userName, password);
    }

    /**
     * Create an authenticator for a Cloud Pak for Data standalone configuration
     * security service from a service definition.
     * 
     * <p>Once created, the caller should use {@link #apply} to get the contents
     * for an {@code Authentication} header.
     *
     * <p>Note that an authenticator created with this method may not be able
     * to re-authenticate as it uses the token from the input service and
     * does not have the user and password information to re-authenticate.
     *
     * @param service JSON service definition. See {@link #config(boolean)} for
     * details on the service definition object.
     * 
     * @return An instance of StandaloneAuthenticator.
     */
    public static StandaloneAuthenticator of(JsonObject service) {
        String endpoint = jstring(jobject(service, CONNECTION_INFO), SERVICE_REST_ENDPOINT);
        Objects.requireNonNull(endpoint);

        StandaloneAuthenticator authenticator = new StandaloneAuthenticator(endpoint, null, null);

        String serviceToken = jstring(service, SERVICE_TOKEN);
        if (serviceToken != null) {
            authenticator.serviceAuth = RestUtils.createBearerAuth(serviceToken);
            authenticator.expire = service.get(SERVICE_TOKEN_EXPIRE).getAsLong();
        }
        authenticator.cfg = service;

        return authenticator;
    }

    /**
     * Get the contents for an {@code Authentication} header.
     * 
     * <p>The authentication returned may only be valid for a short time,
     * depending on the configuration of the security service. As such it
     * is recommended not to retain and use this value over long periods
     * and instead call apply() for each request.
     * 
     * @param executor The Executor to use to make REST requests to the
     * security service, if required.
     * @return Contents for an {@code Authentication} header or null;
     */
    @Override
    public String apply(Executor executor) {
        if (serviceAuth == null || System.currentTimeMillis() > expire) {
            try {
                refreshAuth(executor, cfg);
            } catch (IOException ignored) {
                // Leave as-is and let use fail with auth error
            }
        }

        return serviceAuth;
    }

    StandaloneAuthenticator(String resourcesUrl, String userName, String password) {
        this.resourcesUrl = resourcesUrl;
        this.userName = userName;
        this.password = password;
        this.securityUrl = null;
        this.serviceAuth = null;
        this.expire = 0;
        this.cfg = null;
    }

    private void refreshAuth(Executor executor, JsonObject config) throws IOException {
        Request post = Request.Post(securityUrl)
                .addHeader("Authorization", RestUtils.createBasicAuth(userName, password))
                .addHeader("Accept", ContentType.APPLICATION_JSON.getMimeType())
                .bodyString(AUDIENCE_STREAMS, ContentType.APPLICATION_JSON);

        JsonObject resp = RestUtils.requestGsonResponse(executor, post);
        String token = jstring(resp, ACCESS_TOKEN);
        serviceAuth = token == null ? null : RestUtils.createBearerAuth(token);
        if (resp.has(EXPIRE_TIME)) {
            JsonElement je = resp.get(EXPIRE_TIME);
            // Response is in seconds since epoch, and docs say the min
            // for the service is 30s, so give a 10s of slack if we can
            expire = je.isJsonNull() ? 0 : Math.max(0, je.getAsLong() - 10) * MS;
        } else {
            // Short expiry (same as python)
            expire = System.currentTimeMillis() + 4 * 60 * MS;
        }

        // Update config
        config.addProperty(SERVICE_TOKEN, token);
        config.addProperty(SERVICE_TOKEN_EXPIRE, expire);
    }

    /**
     * Configure the authenticator and create service definition information.
     * 
     * <p>The first time config() is called it contacts the endpoint it was
     * created with to get information about the security service and other
     * related service endpoints and creates a service definition. Later calls
     * will return this saved service defition.
     * 
     * <p>The service definition is a JSON object of the form:
     * <pre>
     * {
     *   "type" : "streams",
     *   "externalClient" : "true",
     *   "service_token" : "...",
     *   "service_token_expire" : T,
     *   "connection_info" : {
     *     "serviceRestEndpoint" : "...",
     *     "serviceBuildEndpoint" : "..."
     *   }
     * }
     * </pre>
     * where the token expiry time is in milliseconds from the UNIX Epoch, and
     * the connection endpoints are URLs. Only service endpoints that were found
     * are included, so if the required endpoint is not found here it must be
     * determined some other way (eg. via an environment variable).
     * 
     * <p>The method may return null if there is no configured security service
     * or the authenticator was unable to use it, or to find other services.
     *
     * @param verify Verify the TLS / SSL certificate for the request.
     * @return A JSON service definition or {@code null}.
     * 
     * @throws IOException
     */
    public JsonObject config(boolean verify) throws IOException {
        if (cfg != null) {
            return cfg;
        }

        return config(RestUtils.createExecutor(!verify));
    }

    private JsonObject config(Executor executor) throws IOException {
        if (cfg != null) {
            return cfg;
        }

        // Try to discover security service and instance URLs
        String streamsEndpoint = null;
        String buildsEndpoint = null;
        try {
            String instancesUrl = null;
            String basicAuth = RestUtils.createBasicAuth(userName, password);
            JsonObject resp = RestUtils.getGsonResponse(executor, basicAuth, resourcesUrl);
            if (resp == null || !resp.has(RESOURCES)) {
                // Not a standalone instance, return null to allow fallback
                // to basic authentication.
                return null;
            }
            ResourcesArray resources = new GsonBuilder()
                    .excludeFieldsWithoutExposeAnnotation()
                    .create().fromJson(resp, ResourcesArray.class);
            for (Resource resource : resources.resources) {
                if (ACCESS_TOKENS_RESOURCE.equals(resource.name)) {
                    securityUrl = resource.resource;
                } else if (INSTANCES_RESOURCE.equals(resource.name)) {
                    instancesUrl = resource.resource;
                } else if (BUILDS_RESOURCE.equals(resource.name)) { 
                    buildsEndpoint = null;
                }
            }

            if (instancesUrl != null) {
                resp = RestUtils.getGsonResponse(executor, basicAuth, instancesUrl);
                JsonArray instances = resp.getAsJsonArray(INSTANCES_ARRAY);
                if (instances != null && instances.size() == 1) {
                    streamsEndpoint = jstring(instances.get(0).getAsJsonObject(), SELF);
                }
            }
        } catch (IOException ignored) {}

        if (securityUrl == null || (streamsEndpoint == null && buildsEndpoint == null)) {
            // Need to have a security URL and at least one of streams or
            // build endpoint, return null to allow fallback to basic auth.
            return null;
        }

        JsonObject config = new JsonObject();
        config.addProperty("type", "streams");
        config.addProperty("externalClient", true);

        try {
            refreshAuth(executor, config);
        } catch (IOException e) {
            // If this fails first time it may be because the security service
            // URL was not resolvable / reachable (eg. an internal URL was
            // given instead of an external one), or there were other I/O
            // problems. In either case, return null to allow fallback to
            // basic authentication.
            return null;
        }

        JsonObject connInfo = new JsonObject();
        if (streamsEndpoint != null) {
            connInfo.addProperty(SERVICE_REST_ENDPOINT, streamsEndpoint);
        }
        if (buildsEndpoint != null) {
            connInfo.addProperty(SERVICE_BUILD_ENDPOINT, buildsEndpoint);
        }
        config.add(CONNECTION_INFO, connInfo);

        cfg = config;
        return config;
    }

    private static class Resource {
        @Expose
        public String name;

        @Expose
        public String resource;
    }

    private static class ResourcesArray {
        @Expose
        public ArrayList<Resource> resources;
    }
}
