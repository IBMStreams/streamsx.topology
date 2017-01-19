/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017  
 */
package com.ibm.streamsx.topology.internal.streaminganalytics;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import com.ibm.streamsx.topology.context.remote.RemoteContext;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

import javax.xml.bind.DatatypeConverter;

public class RestUtils {

    public static String getStatusURL(JsonObject credentials) {
        StringBuilder sb = new StringBuilder(500);
        sb.append(jstring(credentials, "rest_url"));
        sb.append(jstring(credentials, "status_path"));
        return sb.toString();
    }

    public static void checkInstanceStatus(CloseableHttpClient httpClient, JsonObject credentials)
            throws ClientProtocolException, IOException {

        String url = getStatusURL(credentials);

	String apiKey = getAPIKey(jstring(credentials,  "userid"), jstring(credentials, "password"));

        HttpGet getStatus = new HttpGet(url);
        getStatus.addHeader(AUTH.WWW_AUTH_RESP, apiKey);

	JsonObject jsonResponse = getGsonResponse(httpClient, getStatus);
        
        RemoteContext.REMOTE_LOGGER.info("Streaming Analytics Service instance status response:" + jsonResponse.toString());
        
        if (!"true".equals(jstring(jsonResponse, "enabled")))
            throw new IllegalStateException("Service is not enabled!");
        
        if (!"running".equals(jstring(jsonResponse, "status")))
            throw new IllegalStateException("Service is not running!");
    }

    public static String getAPIKey(String userid, String password) throws UnsupportedEncodingException{
	String api_creds = userid + ":" + password;
	String apiKey = "Basic "
	    + DatatypeConverter.printBase64Binary(api_creds
						  .getBytes("UTF-8"));			
	return apiKey;
    }

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
}