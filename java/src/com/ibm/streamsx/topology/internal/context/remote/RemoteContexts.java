package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

class RemoteContexts {
    static JsonObject getGsonResponse(CloseableHttpClient httpClient,
            HttpRequestBase request) throws IOException, ClientProtocolException {
        request.addHeader("accept",
                ContentType.APPLICATION_JSON.getMimeType());

        CloseableHttpResponse response = httpClient.execute(request);
        JsonObject jsonResponse;
        try {
            HttpEntity entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                final String errorInfo;
                if (entity != null)
                    errorInfo = " -- " + EntityUtils.toString(entity);
                else
                    errorInfo = "";
                throw new IllegalStateException(
                        "Unexpected HTTP resource from service:"
                                + response.getStatusLine().getStatusCode() + ":" +
                                response.getStatusLine().getReasonPhrase() + errorInfo);
            }
            
            if (entity == null)
                throw new IllegalStateException("No HTTP resource from service");

            
            Reader r = new InputStreamReader(entity.getContent());
            jsonResponse = new Gson().fromJson(r, JsonObject.class);
            EntityUtils.consume(entity);
        } finally {
            response.close();
        }
        return jsonResponse;
    }
    

    static JsonObject getVCAPService(JsonObject deploy) throws IOException {
        JsonObject services = object(deploy, VCAP_SERVICES);
        JsonArray streamsServices = array(services, "streaming-analytics");
        if (streamsServices == null)
            throw new IllegalStateException("No streaming-analytics services defined in VCAP_SERVICES");
        
        String serviceName = jstring(deploy, SERVICE_NAME);
        
        JsonObject service = null;
        for (JsonElement je : streamsServices) {
            JsonObject possibleService = je.getAsJsonObject();
            if (serviceName.equals(GsonUtilities.jstring(possibleService, "name"))) {
                service = possibleService;
                break;
            }
        }
        if (service == null)
            throw new IllegalStateException(
                    "No streaming-analytics services defined in VCAP_SERVICES with name: " + serviceName);
        
        return service;
    }
}
