/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.streamsrest;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

interface StreamsKeys {
    
    String SERVICE_DEFINITION = "topology.service.definition";
    
    String CONNECTION_INFO = "connection_info";
    String BUILD_SERVICE_URL = "serviceBuildUrl";
    String BEARER_TOKEN = "bearerToken";
    String STREAMS_REST_URL = "streamsRestUrl";
    
    static String getBuildServiceURL(JsonObject deploy) {
        return getConnectionInfo(deploy, BUILD_SERVICE_URL);
    }
    static String getStreamsInstanceURL(JsonObject deploy) {
        return getConnectionInfo(deploy, STREAMS_REST_URL);
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

}
