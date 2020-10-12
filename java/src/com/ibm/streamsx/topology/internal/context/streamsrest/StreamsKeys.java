/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import java.io.IOException;
import java.net.URL;
import java.util.Objects;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public interface StreamsKeys {

    String SERVICE_DEFINITION = "topology.service.definition";

    String CONNECTION_INFO = "connection_info";
    String BUILD_SERVICE_ENDPOINT = "serviceBuildEndpoint";
    String BUILD_PATH = "/streams/rest/builds";
    String BEARER_TOKEN = "service_token";
    String PRODUCT_VERSION = "productVersion";
    // serviceRestEndpoint points directly to the instance URL
    String STREAMS_REST_ENDPOINT = "serviceRestEndpoint";
    String STREAMS_REST_RESOURCES_ENDPOINT = "serviceRestResourceEndpoint";
    String INSTANCES_PATH = "/streams/rest/instances/";

    static String getBuildServiceURL(JsonObject deploy) throws Exception {
        String url = getConnectionInfo(deploy, BUILD_SERVICE_ENDPOINT);        
        url = Objects.requireNonNull(url);

        URL u = new URL(url);
        if (!BUILD_PATH.equals(u.getPath()))
            throw new IllegalStateException("Expecting build service endpoint with path " + BUILD_PATH + " : " + BUILD_SERVICE_ENDPOINT + "=" + url);

        return url;
    }

    /**
     * Gets the REST resources URL if any.
     * @param deploy the deploy JSON object
     * @return the URL as a String or null
     * @throws IOException
     */
    static String getStreamsRestResourcesUrl(JsonObject deploy) throws IOException {
        String url = getConnectionInfo(deploy, STREAMS_REST_RESOURCES_ENDPOINT);
        return url;
    }

    static String getStreamsInstanceURL(JsonObject deploy) throws IOException {
        String url = getConnectionInfo(deploy, STREAMS_REST_ENDPOINT);
        url = Objects.requireNonNull(url);
        return url;
    }

    static String getConnectionInfo(JsonObject deploy, String key) {
        JsonObject service = deploy.get(SERVICE_DEFINITION).getAsJsonObject();

        if (service.has(CONNECTION_INFO)) {
            JsonObject connInfo = GsonUtilities.jobject(service, CONNECTION_INFO);
            if (connInfo != null) {
                return GsonUtilities.jstring(connInfo, key);
            }            
        }

        return null;
    }

    static String getBearerToken(JsonObject deploy) {
        JsonObject service = deploy.get(SERVICE_DEFINITION).getAsJsonObject();

        return GsonUtilities.jstring(service, BEARER_TOKEN);
    }
    
    /**
     * Returns the productVersion from the topology.service.definition object
     * @param deploy the deploy JSON object
     * @return the product version, for example 5.3.1.0 or <tt>null</tt> when not present.
     */
    static String getProductVersion(JsonObject deploy) {
        JsonObject service = deploy.get(SERVICE_DEFINITION).getAsJsonObject();
        return GsonUtilities.jstring(service, PRODUCT_VERSION);
    }
}
