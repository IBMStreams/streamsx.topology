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

public class StandaloneAuthenticator implements Function<Executor,String> {
    private String resourcesUrl;
    private String userName;
    private String password;
    private String securityUrl;
    private String auth;
    private long expire;    // in ms since epoch

    private JsonObject cfg;

    // Resources URL path
    private static final String STREAMS_REST_RESOURCES = "/streams/rest/resources";
    // Resources response array
    private static final String RESOURCES = "resources";
    // Resource name for security service URL
    private static final String ACCESS_TOKENS = "accessTokens";
    // Resrouce name for instances URL
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

    private static long MS = 1000;

    public static StandaloneAuthenticator of(String endpoint, String userName, String password) {
        if (endpoint == null) {
            endpoint = System.getenv(Util.ICP4D_DEPLOYMENT_URL);
            if (endpoint == null) {
                endpoint = System.getenv(Util.STREAMS_REST_URL);
            }
        }

        // Fix endpoint if necessary
        endpoint = getResourcesUrl(endpoint);

        // Fill in user / password defaults if required
        if (userName == null || password == null) {
            String[] values = Util.getDefaultUserPassword(userName, password);
            userName = values[0];
            password = values[1];
        }

        return new StandaloneAuthenticator(endpoint, userName, password);
    }

    public static StandaloneAuthenticator of(JsonObject service) {
        String endpoint = jstring(jobject(service, CONNECTION_INFO), SERVICE_REST_ENDPOINT);
        Objects.requireNonNull(endpoint);

        StandaloneAuthenticator authenticator = new StandaloneAuthenticator(endpoint, null, null);

        String serviceToken = jstring(service, SERVICE_TOKEN);
        if (serviceToken != null) {
            authenticator.auth = RestUtils.createBearerAuth(serviceToken);
            authenticator.expire = service.get(SERVICE_TOKEN_EXPIRE).getAsLong();
        }
        authenticator.cfg = service;

        return authenticator;
    }

    @Override
    public String apply(Executor executor) {
        if (auth == null || System.currentTimeMillis() > expire) {
            try {
                refreshAuth(executor);
            } catch (IOException ignored) {
                // Leave as-is and let use fail with auth error
            }
        }
        return auth;
    }

    public String getResourcesUrl() {
        return resourcesUrl;
    }

    StandaloneAuthenticator(String resourcesUrl, String userName, String password) {
        this.resourcesUrl = resourcesUrl;
        this.userName = userName;
        this.password = password;
        this.securityUrl = null;
        this.auth = null;
        this.expire = 0;
        this.cfg = null;
    }

    private String refreshAuth(Executor executor) throws IOException {
        Request post = Request.Post(securityUrl)
                .addHeader("Authorization", RestUtils.createBasicAuth(userName, password))
                .addHeader("Accept", ContentType.APPLICATION_JSON.getMimeType())
                .bodyString(AUDIENCE_STREAMS, ContentType.APPLICATION_JSON);

        JsonObject resp = RestUtils.requestGsonResponse(executor, post);
        String token = jstring(resp, ACCESS_TOKEN);
        auth = token == null ? null : RestUtils.createBearerAuth(token);
        if (resp.has(EXPIRE_TIME)) {
            JsonElement je = resp.get(EXPIRE_TIME);
            // Response is in seconds since epoch, and docs say the min
            // for the service is 30s, so give a 05s of slack if we can
            expire = je.isJsonNull() ? 0 : Math.max(0, je.getAsLong() - 10) * MS;
        } else {
            // Short expiry (same as python)
            expire = System.currentTimeMillis() + 4 * 60 * MS;
        }

        return token;
    }

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
        try {
            String instancesUrl = null;
            String basicAuth = RestUtils.createBasicAuth(userName, password);
            JsonObject resp = RestUtils.getGsonResponse(executor, basicAuth, resourcesUrl);
            if (resp == null || !resp.has(RESOURCES)) {
                // Not a standalone instance
                return null;
            }
            ResourcesArray resources = new GsonBuilder()
                    .excludeFieldsWithoutExposeAnnotation()
                    .create().fromJson(resp, ResourcesArray.class);
            for (Resource resource : resources.resources) {
                if (ACCESS_TOKENS.equals(resource.name)) {
                    securityUrl = resource.resource;
                } else if (INSTANCES_RESOURCE.equals(resource.name)) {
                    instancesUrl = resource.resource;
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

        if (securityUrl == null || streamsEndpoint == null) {
            // Unable to configure, return null to indicate may not be a
            // standalone instance
            return null;
        }

        String token = refreshAuth(executor);

        JsonObject config = new JsonObject();

        config.addProperty("type", "streams");
        config.addProperty("externalClient", true);
        config.addProperty(SERVICE_TOKEN, token);
        config.addProperty(SERVICE_TOKEN_EXPIRE, expire);

        URL securityUrl = new URL(resourcesUrl);
        config.addProperty("cluster_ip", securityUrl.getHost());
        config.addProperty("cluster_port", securityUrl.getPort());

        // Get service rest endpoint from instancesUrl
        
        JsonObject connInfo = new JsonObject();
        connInfo.addProperty(SERVICE_REST_ENDPOINT, streamsEndpoint);
        config.add(CONNECTION_INFO, connInfo);

        cfg = config;
        return config;
    }

    private static String getResourcesUrl(String endpoint) {
        if (!endpoint.endsWith(STREAMS_REST_RESOURCES)) {
            try {
                URL url = new URL(endpoint);
                URL restUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), STREAMS_REST_RESOURCES);
                endpoint = restUrl.toExternalForm();
            } catch (MalformedURLException ignored) {
                // Leave as-is
            }
        }
        return endpoint;
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
