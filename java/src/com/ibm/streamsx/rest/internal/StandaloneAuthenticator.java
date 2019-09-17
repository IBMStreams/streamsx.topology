/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019 
 */
package com.ibm.streamsx.rest.internal;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
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

    private static final String STREAMS_REST_RESOURCES = "/streams/rest/resources";
    private static final String RESOURCES = "resources";
    private static final String ACCESS_TOKENS = "accessTokens";
    private static final String AUDIENCE_STREAMS = "{\"audience\":[\"streams\"]}";
    private static final String EXPIRE_TIME = "expireTime";
    private static final String ACCESS_TOKEN = "accessToken";
    private static long MS = 1000;

    public static StandaloneAuthenticator of(String endpoint, String userName, String password) {
        if (endpoint == null) {
            endpoint = System.getenv(Util.ICP4D_DEPLOYMENT_URL);
            if (endpoint == null) {
                endpoint = System.getenv(Util.STREAMS_REST_URL);
            }
        }

        // Fix up endpoint if necessary to get resources URL
        if (!endpoint.endsWith(STREAMS_REST_RESOURCES)) {
            try {
                URL url = new URL(endpoint);
                URL restUrl = new URL(url.getProtocol(), url.getHost(), url.getPort(), STREAMS_REST_RESOURCES);
                endpoint = restUrl.toExternalForm();
            } catch (MalformedURLException ignored) {
                // Leave as-is
            }
        }

        // Fill in user / password defaults if required
        if (userName == null || password == null) {
            String[] values = Util.getDefaultUserPassword(userName, password);
            userName = values[0];
            password = values[1];
        }

        return new StandaloneAuthenticator(endpoint, userName, password);
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

    private void refreshAuth(Executor executor) throws IOException {
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

        // Try to discover security service and build URLs
        try {
            Request get = Request.Get(resourcesUrl)
                    .addHeader("Authorization", RestUtils.createBasicAuth(userName, password))
                    .addHeader("Accept", ContentType.APPLICATION_JSON.getMimeType());
            JsonObject resp = RestUtils.requestGsonResponse(executor, get);
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
                    break;
                }
            }
        } catch (IOException ignored) {}

        if (securityUrl == null) {
            // Unable to configure, return null to indicate may not be a
            // standalone instance
            return null;
        }

        refreshAuth(executor);

        JsonObject config = new JsonObject();

        config.addProperty("type", "streams");
        config.addProperty("externalClient", true);
        config.addProperty("service_token", auth);
        config.addProperty("service_token_expire", expire);

        URL securityUrl = new URL(resourcesUrl);
        config.addProperty("cluster_ip", securityUrl.getHost());
        config.addProperty("cluster_port", securityUrl.getPort());

        JsonObject connInfo = new JsonObject();       
        connInfo.addProperty("serviceRestEndpoint", resourcesUrl);
        config.add("connection_info", connInfo);

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
