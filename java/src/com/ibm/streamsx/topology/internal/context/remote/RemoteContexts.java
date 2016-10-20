package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

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

public class RemoteContexts {
    public static JsonObject getGsonResponse(CloseableHttpClient httpClient,
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
    
	public static Map<String, Object> gsonDeployToMap(JsonObject deploy) {
		Map<String, Object> config = new HashMap<>();
		if(deploy.has(VCAP_SERVICES))
			config.put(VCAP_SERVICES, GsonUtilities.object(deploy, VCAP_SERVICES));
		if(deploy.has(SERVICE_NAME))
			config.put(SERVICE_NAME, GsonUtilities.jstring(deploy, SERVICE_NAME));
		return config;
	}


    public static JsonObject getVCAPService(Map<String, Object> config) throws IOException {
        JsonObject services = (JsonObject)config.get(VCAP_SERVICES);
        JsonArray streamsServices = GsonUtilities.array(services, "streaming-analytics");
        if (streamsServices == null || streamsServices.size() == 0)
            throw new IllegalStateException("No streaming-analytics services defined in VCAP_SERVICES");
        
        String serviceName = config.get(SERVICE_NAME).toString();
        
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
