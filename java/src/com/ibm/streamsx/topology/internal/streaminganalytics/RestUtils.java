/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016,2017  
 */
package com.ibm.streamsx.topology.internal.streaminganalytics;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices.getVCAPService;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;

public class RestUtils { 

    public static String getStatusURL(JsonObject credentials) {
        StringBuilder sb = new StringBuilder(500);
        sb.append(jstring(credentials, "rest_url"));
        sb.append(jstring(credentials, "status_path"));
        return sb.toString();
    }
    
    public static String getJobSubmitURL(JsonObject credentials, File bundle) throws UnsupportedEncodingException  {
        StringBuilder sb = new StringBuilder(500);
        sb.append(jstring(credentials, "rest_url"));
        sb.append(jstring(credentials, "jobs_path"));
        sb.append("?");
        sb.append("bundle_id=");
        sb.append(URLEncoder.encode(bundle.getName(), StandardCharsets.UTF_8.name()));
        return sb.toString();
    }

    public static void checkInstanceStatus(CloseableHttpClient httpClient, JsonObject service)
            throws ClientProtocolException, IOException {
        final String serviceName = jstring(service, "name");
        final JsonObject credentials = service.getAsJsonObject("credentials");
        
        String url = getStatusURL(credentials);

        String apiKey = getAPIKey(credentials);

        HttpGet getStatus = new HttpGet(url);
        getStatus.addHeader(AUTH.WWW_AUTH_RESP, apiKey);

        JsonObject jsonResponse = getGsonResponse(httpClient, getStatus);
        
        RemoteContext.REMOTE_LOGGER.info("Streaming Analytics service (" + serviceName + "): instance status response:" + jsonResponse.toString());
        
        if (!"true".equals(jstring(jsonResponse, "enabled")))
            throw new IllegalStateException("Service is not enabled!");
        
        if (!"running".equals(jstring(jsonResponse, "status")))
            throw new IllegalStateException("Service is not running!");
    }

    public static String getAPIKey(JsonObject credentials) {
        String userid = jstring(credentials,  "userid");
        String password = jstring(credentials, "password");
        
        String api_creds = userid + ":" + password;
        String apiKey = "Basic " + DatatypeConverter.printBase64Binary(api_creds.getBytes(StandardCharsets.UTF_8));
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
	
	/**
	 * Submit an application bundle to execute as a job.
	 */
    public static JsonObject postJob(CloseableHttpClient httpClient, JsonObject service, File bundle,
            JsonObject jobConfigOverlay) throws ClientProtocolException, IOException {
        
        final String serviceName = jstring(service, "name");
        final JsonObject credentials = service.getAsJsonObject("credentials");
        
        String url = getJobSubmitURL(credentials, bundle);

        HttpPost postJobWithConfig = new HttpPost(url);
        postJobWithConfig.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
        postJobWithConfig.addHeader(AUTH.WWW_AUTH_RESP, getAPIKey(credentials));
        FileBody bundleBody = new FileBody(bundle, ContentType.APPLICATION_OCTET_STREAM);
        StringBody configBody = new StringBody(jobConfigOverlay.toString(), ContentType.APPLICATION_JSON);

        HttpEntity reqEntity = MultipartEntityBuilder.create().addPart("sab", bundleBody)
                .addPart(DeployKeys.JOB_CONFIG_OVERLAYS, configBody).build();

        postJobWithConfig.setEntity(reqEntity);

        JsonObject jsonResponse = getGsonResponse(httpClient, postJobWithConfig);

        RemoteContext.REMOTE_LOGGER.info("Streaming Analytics service (" + serviceName + "): submit job response:" + jsonResponse.toString());

        return jsonResponse;
    }
}