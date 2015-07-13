/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

public class AnalyticsServiceStreamsContext extends
        BundleUserStreamsContext<BigInteger> {

    public AnalyticsServiceStreamsContext() {
        super(false);
    }

    @Override
    public Type getType() {
        return Type.ANALYTICS_SERVICE;
    }

    @Override
    public Future<BigInteger> submit(Topology app, Map<String, Object> config)
            throws Exception {

        preBundle(config);
        File bundle = bundler.submit(app, config).get();

        preInvoke();

        BigInteger jobId = submitJobToService(bundle, config);
        
        return new CompletedFuture<BigInteger>(jobId);
    }
    
    void preInvoke() {
        
    }
    
    void preBundle(Map<String, Object> config) {
        if (!config.containsKey(SERVICE_NAME))
            throw new IllegalStateException("Service name is not defined, please set property: " + SERVICE_NAME);
        
        if (!config.containsKey(VCAP_SERVICES)) {
            throw new IllegalStateException("VCAP services are not defined, please set property: " + VCAP_SERVICES);
        }
    }
    
    private JSONObject getVCAPServices(Map<String, Object> config) throws IOException {
        
        Object rawServices = config.get(VCAP_SERVICES);
        if (rawServices instanceof File) {
            File fServices = (File) rawServices;
            
            try (FileInputStream fis = new FileInputStream(fServices)) {
                return JSONObject.parse(fis);
            }
            
        } else {
            throw new IllegalArgumentException();
        }       
    }
    
    private JSONObject getVCAPService(Map<String, Object> config) throws IOException {
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
    
    private CloseableHttpClient createHttpClient(JSONObject credentials) {
        
        
        UsernamePasswordCredentials upc = new UsernamePasswordCredentials(
                credentials.get("userid").toString(),
                credentials.get("password").toString());
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope(credentials.get("rest_host").toString(),  AuthScope.ANY_PORT),
                upc);
        CloseableHttpClient httpClient = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build();
        return httpClient;
    }
    
    private String getStatusURL(JSONObject credentials) {
        StringBuilder sb = new StringBuilder(500);
        sb.append(credentials.get("rest_url"));
        sb.append(credentials.get("status_path"));
        return sb.toString();
    }
    
    private void checkInstanceStatus(CloseableHttpClient httpClient, JSONObject credentials)
            throws ClientProtocolException, IOException {

        String url = getStatusURL(credentials);

        HttpGet getStatus = new HttpGet(url);
        JSONObject jsonResponse = getJsonResponse(httpClient, getStatus);
        
        System.out.println("STATUS=" + jsonResponse);
        
        if (!Boolean.TRUE.equals(jsonResponse.get("enabled")))
            throw new IllegalStateException("Service is not enabled!");
        
        if (!"running".equals(jsonResponse.get("status")))
            throw new IllegalStateException("Service is not running!");
    }

    private JSONObject getJsonResponse(CloseableHttpClient httpClient,
            HttpRequestBase request) throws IOException, ClientProtocolException {
        request.addHeader("accept",
                ContentType.APPLICATION_JSON.getMimeType());

        CloseableHttpResponse response = httpClient.execute(request);
        JSONObject jsonResponse;
        try {
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
                throw new IllegalStateException(
                        "Unexpected HTTP resource from service:"
                                + response.getStatusLine().getStatusCode());
            HttpEntity entity = response.getEntity();
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
    
    private String getSubmitURL(JSONObject credentials, File bundle) throws UnsupportedEncodingException  {
        StringBuilder sb = new StringBuilder(500);
        sb.append(credentials.get("rest_url"));
        sb.append(credentials.get("jobs_path"));
        sb.append("?");
        sb.append("bundle_id=");
        sb.append(URLEncoder.encode(bundle.getName(), "UTF-8"));
        return sb.toString();
    }
    
    private void postJob(CloseableHttpClient httpClient, JSONObject credentials, File bundle)
            throws ClientProtocolException, IOException {

        JSONObject submitConfig = new JSONObject();
        String url = getSubmitURL(credentials, bundle);
        
        HttpPost postJobWithConfig = new HttpPost(url);
        postJobWithConfig.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());

        FileBody bundleBody = new FileBody(bundle,
                ContentType.APPLICATION_OCTET_STREAM);
        StringBody configBody = new StringBody(submitConfig.serialize(),
                ContentType.APPLICATION_JSON);

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("bin", bundleBody).addPart("json", configBody)
                .build();

        postJobWithConfig.setEntity(reqEntity);

        JSONObject jsonResponse = getJsonResponse(httpClient, postJobWithConfig);
        
        System.out.println("SUBMIT=" + jsonResponse);
    }
    
    private BigInteger submitJobToService(File bundle, Map<String, Object> config) throws IOException {
        JSONObject service = getVCAPService(config);
        
        final JSONObject credentials = (JSONObject) service.get("credentials");
        
        final CloseableHttpClient httpClient = createHttpClient(credentials);
        try {
            checkInstanceStatus(httpClient, credentials);
            postJob(httpClient, credentials, bundle);
        } finally {
            httpClient.close();
        }
        
        return null;
    }
}
