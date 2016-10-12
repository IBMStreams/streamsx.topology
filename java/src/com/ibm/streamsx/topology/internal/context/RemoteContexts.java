package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;

public class RemoteContexts {
	static void preBundle(Map<String, Object> config) {
        if (!config.containsKey(SERVICE_NAME))
            throw new IllegalStateException("Service name is not defined, please set property: " + SERVICE_NAME);
        
        if (!config.containsKey(VCAP_SERVICES)) {
            throw new IllegalStateException("VCAP services are not defined, please set property: " + VCAP_SERVICES);
        }
    }
    
    private static JSONObject getVCAPServices(Map<String, Object> config) throws IOException {
        
        Object rawServices = config.get(VCAP_SERVICES);
        if (rawServices instanceof File) {
            File fServices = (File) rawServices;
            
            try (FileInputStream fis = new FileInputStream(fServices)) {
                return JSONObject.parse(fis);
            }
            
        } 
        else if (rawServices instanceof String) {
        	return JSONObject.parse((String)rawServices);
        }
        else {
            throw new IllegalArgumentException();
        }       
    }
    public static JSONObject getVCAPService(Map<String, Object> config) throws IOException {
        JSONObject services = getVCAPServices(config);
        JSONArray streamsServices = (JSONArray) services.get("streaming-analytics");
        if (streamsServices == null || streamsServices.isEmpty())
            throw new IllegalStateException("No streaming-analytics services defined in VCAP_SERVICES");
        
        String serviceName = config.get(SERVICE_NAME).toString();
        
        JSONObject service = null;
        for (Object jo : streamsServices) {
            JSONObject possibleService = (JSONObject) jo;
            if (serviceName.equals(possibleService.get("name"))) {
                service = possibleService;
                break;
            }
        }
        if (service == null)
            throw new IllegalStateException(
                    "No streaming-analytics services defined in VCAP_SERVICES with name: " + serviceName);
        
        return service;
    }
    
    static JSONObject getJsonResponse(CloseableHttpClient httpClient,
            HttpRequestBase request) throws IOException, ClientProtocolException {
        request.addHeader("accept",
                ContentType.APPLICATION_JSON.getMimeType());

        CloseableHttpResponse response = httpClient.execute(request);
        JSONObject jsonResponse;
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

            jsonResponse = JSONObject.parse(new BufferedInputStream(entity
                    .getContent()));
            EntityUtils.consume(entity);
        } finally {
            response.close();
        }
        return jsonResponse;
    }

	public static Map<String, Object> jsonDeployToMap(JSONObject deploy) {
		Map<String, Object> config = new HashMap<>();
		if(deploy.containsKey(VCAP_SERVICES))
			config.put(VCAP_SERVICES, deploy.get(VCAP_SERVICES));
		if(deploy.containsKey(SERVICE_NAME))
			config.put(SERVICE_NAME, deploy.get(SERVICE_NAME));
		return config;
	}
}
