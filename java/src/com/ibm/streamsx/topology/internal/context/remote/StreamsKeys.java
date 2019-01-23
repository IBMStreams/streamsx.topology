/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
 */
package com.ibm.streamsx.topology.internal.context.remote;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public interface StreamsKeys {
    
    String SERVICE_DEFINITION = "topology.service.definition";
    
    String CONNECTION_INFO = "connection_info";
    String BUILD_SERVICE_URL = "serviceBuildUrl";
    String BEARER_TOKEN = "bearerToken";
    
    static String getBuildServiceURL(JsonObject deploy) {
        System.err.println("DEPLOY:" + deploy);
        
        JsonObject service = deploy.get(SERVICE_DEFINITION).getAsJsonObject();
        
        if (service.has(CONNECTION_INFO)) {
            JsonObject connInfo = GsonUtilities.jobject(service, CONNECTION_INFO);
            if (connInfo != null) {
                String url = GsonUtilities.jstring(connInfo, BUILD_SERVICE_URL);
                if (url != null)
                    return url;
            }            
        }
        
        return System.getenv("STREAMS_BUILDSERVICE_REST_URL");
    }
    
    static String getBearerToken(JsonObject deploy) {
        JsonObject service = deploy.get(SERVICE_DEFINITION).getAsJsonObject();
        
        return GsonUtilities.jstring(service, BEARER_TOKEN);
    }

}
