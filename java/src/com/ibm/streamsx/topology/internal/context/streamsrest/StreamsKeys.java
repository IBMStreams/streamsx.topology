/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import java.util.Objects;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public interface StreamsKeys {

    String SERVICE_DEFINITION = "topology.service.definition";
    String SERVICE_NAME = "service_name";
    String SERVICE_NAMESPACE = "service_namespace";
    String SERVICE_ID = "service_id";
    String SPACE_NAME = "topology.spaceName";

    String CONNECTION_INFO = "connection_info";
    String CLUSTER_IP = "cluster_ip";
    String CLUSTER_PORT = "cluster_port";
    String EXTERNAL_CLIENT = "externalClient";
    String BUILD_SERVICE_ENDPOINT = "serviceBuildEndpoint";
    String BUILD_PATH = "/streams/rest/builds";
    String BEARER_TOKEN = "service_token";
    String USER_TOKEN = "user_token";
    String PRODUCT_VERSION = "productVersion";
    // serviceRestEndpoint points directly to the instance URL
    String STREAMS_REST_ENDPOINT = "serviceRestEndpoint";
    String STREAMS_REST_RESOURCES_ENDPOINT = "serviceRestResourceEndpoint";
    String INSTANCES_PATH = "/streams/rest/instances/";

    /**
     * Gets the REST URL 'serviceBuildEndpoint' from the 'deploy'->'topology.service.definition'->'connection_info' JSON object.
     * @param deploy the deploy JSON object
     * @return the URL as a String
     * @throws NullPointerException the 'serviceBuildEndpoint' is not present in the 'connection_info'
     */
    static String getBuildServiceURL(JsonObject deploy) throws Exception {
        String url = getConnectionInfo(deploy, BUILD_SERVICE_ENDPOINT);
        url = Objects.requireNonNull(url);
        return url;
    }

    /**
     * Gets the REST resources URL 'serviceRestResourceEndpoint' from the 'deploy'->'topology.service.definition'->'connection_info' JSON object if any.
     * @param deploy the deploy JSON object
     * @return the URL as a String or null
     */
    static String getStreamsRestResourcesUrl(JsonObject deploy) {
        String url = getConnectionInfo(deploy, STREAMS_REST_RESOURCES_ENDPOINT);
        return url;
    }

    /**
     * Gets the REST URL 'serviceRestEndpoint' from the 'deploy'->'topology.service.definition'->'connection_info' JSON object.
     * @param deploy the deploy JSON object
     * @return the URL as a String
     * @throws NullPointerException the 'serviceRestEndpoint' is not present in the 'connection_info'
     */
    static String getStreamsInstanceURL(JsonObject deploy) {
        String url = getConnectionInfo(deploy, STREAMS_REST_ENDPOINT);
        // throws NullPointerException on url == null:
        url = Objects.requireNonNull(url);
        return url;
    }

    /**
     * Get a String element from the 'deploy'->'topology.service.definition'->'connection_info', addressed by a key.
     * 
     * @param deploy the 'deploy' JSON object
     * @param key the key of the element, for example "serviceRestEndpoint"
     * @return the value or null if either the 'connection_info' or the element addressed by key does not exist. 
     */
    static String getConnectionInfo(JsonObject deploy, String key) {
        JsonObject service = deploy.get(SERVICE_DEFINITION).getAsJsonObject();
        if (service != null && service.has(CONNECTION_INFO)) {
            JsonObject connInfo = GsonUtilities.jobject(service, CONNECTION_INFO);
            if (connInfo != null) {
                return GsonUtilities.jstring(connInfo, key);
            }
        }
        return null;
    }

    /**
     * Get a String element from the 'deploy'->'topology.service.definition' JSON object, addressed by a key.
     * 
     * @param deploy the 'deploy' JSON object
     * @param key the key of the element, for example "service_name"
     * @return the value or null if either the 'topology.service.definition' or the element addressed by key does not exist. 
     */
    static String getFromServiceDefinition(final JsonObject deploy, final String key) {
        JsonObject service = deploy.get (SERVICE_DEFINITION).getAsJsonObject();
        if (service == null) return null;
        return GsonUtilities.jstring (service, key);
    }
    
    /**
     * Get the 'service_token' from the 'deploy'->'topology.service.definition' structure of a 'deploy' JSON object.
     * 
     * @param deploy the 'deploy' JSON object
     * @return the service token or null if the token cannot be found in the JSON object
     */
    static String getBearerToken(JsonObject deploy) {
        return getFromServiceDefinition(deploy, BEARER_TOKEN);
    }

    /**
     * Returns the productVersion from the 'deploy'->'topology.service.definition' object
     * @param deploy the deploy JSON object
     * @return the product version, for example 5.3.1.0 or <tt>null</tt> when not present.
     */
    static String getProductVersion(JsonObject deploy) {
        return getFromServiceDefinition(deploy, PRODUCT_VERSION);
    }
}
