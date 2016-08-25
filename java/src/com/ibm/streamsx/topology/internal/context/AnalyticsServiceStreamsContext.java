/* begin_generated_IBM_copyright_prolog                             */
/*                                                                  */
/* This is an automatically generated copyright prolog.             */
/* After initializing,  DO NOT MODIFY OR MOVE                       */
/* **************************************************************** */
/* IBM Confidential                                                 */
/* OCO Source Materials                                             */
/* 5724-Y95                                                         */
/* (C) Copyright IBM Corp.  2016, 2016                              */
/* The source code for this program is not published or otherwise   */
/* divested of its trade secrets, irrespective of what has          */
/* been deposited with the U.S. Copyright Office.                   */
/*                                                                  */
/* end_generated_IBM_copyright_prolog                               */
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Level;

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
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeSubmit;
import com.ibm.streamsx.topology.jobconfig.JobConfig;
import com.ibm.streamsx.topology.jobconfig.SubmissionParameter;

public class AnalyticsServiceStreamsContext extends
        BundleUserStreamsContext<BigInteger> {
	/* begin_generated_IBM_copyright_code                               */
	public static final String IBM_COPYRIGHT =
		" Licensed Materials-Property of IBM                              " + //$NON-NLS-1$ 
		" 5724-Y95                                                        " + //$NON-NLS-1$ 
		" (C) Copyright IBM Corp.  2016, 2016    All Rights Reserved.     " + //$NON-NLS-1$ 
		" US Government Users Restricted Rights - Use, duplication or     " + //$NON-NLS-1$ 
		" disclosure restricted by GSA ADP Schedule Contract with         " + //$NON-NLS-1$ 
		" IBM Corp.                                                       " + //$NON-NLS-1$ 
		"                                                                 " ; //$NON-NLS-1$ 
	/* end_generated_IBM_copyright_code                                 */

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
        
        Topology.STREAMS_LOGGER.info("Streaming Analytics Service instance status response:" + jsonResponse.serialize());
        
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
    
    private String getSubmitURL(JSONObject credentials, File bundle) throws UnsupportedEncodingException  {
        StringBuilder sb = new StringBuilder(500);
        sb.append(credentials.get("rest_url"));
        sb.append(credentials.get("jobs_path"));
        sb.append("?");
        sb.append("bundle_id=");
        sb.append(URLEncoder.encode(bundle.getName(), "UTF-8"));
        return sb.toString();
    }
    
    private BigInteger postJob(CloseableHttpClient httpClient, JSONObject credentials, File bundle, JSONObject submitConfig)
            throws ClientProtocolException, IOException {

        
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
        
        Topology.STREAMS_LOGGER.info("Streaming Analytics Service submit job response:" + jsonResponse.serialize());
        
        Object jobId = jsonResponse.get("jobId");
        if (jobId == null)
            return BigInteger.valueOf(-1);
        return new BigInteger(jobId.toString());
    }
    
    private JSONObject getBluemixSubmitConfig( Map<String, Object> config) throws IOException {
        JSONObject submitConfig = new JSONObject();
        
        JobConfig jc = JobConfig.fromProperties(config);
        addSubmitValue(submitConfig, jc.getJobName(), "jobName");
        addSubmitValue(submitConfig, jc.getJobGroup(), "jobGroup");
        addSubmitValue(submitConfig, jc.getDataDirectory(), "data-directory");
        if (jc.hasSubmissionParameters()) {
            JSONObject joParams = new JSONObject();
            for(SubmissionParameter param :  jc.getSubmissionParameters()) {
                joParams.put(param.getName(), param.getValue());
            }
            addSubmitValue(submitConfig, joParams, "submissionParameters");
        }

        addSubmitValue(submitConfig, jc.getOverrideResourceLoadProtection(),
                "overrideResourceLoadProtection");
        
        JSONObject submitConfigConfig = new JSONObject();
        if (jc.getPreloadApplicationBundles())
            addSubmitValue(submitConfigConfig, jc.getPreloadApplicationBundles().toString(), "preloadApplicationBundles");
        if (jc.getTracing() != null) {
            submitConfigConfig.put("tracing", jc.getStreamsTracing());
        }
        if (!submitConfigConfig.isEmpty())
            submitConfig.put("configurationSettings", submitConfigConfig);
        
        Topology.STREAMS_LOGGER.info("Streaming Analytics Service submit job request:" + submitConfig.serialize());
        
        return submitConfig;
    }
    
    private static void addSubmitValue(JSONObject json, Object value, String jsonKey) {
        if (value != null)
            json.put(jsonKey, value);
    }
    
    private BigInteger submitJobToService(File bundle, Map<String, Object> config) throws IOException {
        JSONObject service = getVCAPService(config);
        
        final JSONObject credentials = (JSONObject) service.get("credentials");
        
        final CloseableHttpClient httpClient = createHttpClient(credentials);
        try {
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service: Checking status :" + service.get("name"));           
            checkInstanceStatus(httpClient, credentials);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service: Submitting bundle : " + bundle.getName() + " to " + service.get("name"));
            return postJob(httpClient, credentials, bundle, getBluemixSubmitConfig(config));
        } finally {
            httpClient.close();
        }
    }
}
