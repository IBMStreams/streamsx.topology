/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Future;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AUTH;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.AnalyticsServiceProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streaminganalytics.RestUtils;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;
import com.ibm.streamsx.topology.internal.streams.JobConfigOverlay;
import com.ibm.streamsx.topology.jobconfig.JobConfig;

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

    @Override
    public Future<BigInteger> submit(JSONObject submission) throws Exception {
        Map<String, Object> config = Contexts
                .jsonDeployToMap((JSONObject) submission.get(RemoteContext.SUBMISSION_DEPLOY));

        preBundle(config);
        File bundle = bundler.submit(submission).get();
        preInvoke();

        BigInteger jobId = submitJobToService(bundle, config);

        return new CompletedFuture<BigInteger>(jobId);
    }

    void preInvoke() {
        
    }
    
    /**
     * Verify we have a valid Streaming Analytic service
     * information before we attempt anything.
     */
    void preBundle(Map<String, Object> config) {
        try {
            getVCAPService(config);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    JsonObject getVCAPService(Map<String, Object> config) throws IOException {
        // Convert from JSON4J to a string since the common code
        // does not reference JSON4J
        Object vco = config.get(AnalyticsServiceProperties.VCAP_SERVICES);
        if (vco instanceof JSONObject) {
            JSONObject servicej = (JSONObject) vco;
            config.put(AnalyticsServiceProperties.VCAP_SERVICES, servicej.serialize());           
        }
        return VcapServices.getVCAPService(key -> config.get(key));
    }
    
    private CloseableHttpClient createHttpClient(JSONObject credentials) {
	CloseableHttpClient httpClient = HttpClients.custom()
	    .build();
	return httpClient;
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
        sb.append(URLEncoder.encode(bundle.getName(), StandardCharsets.UTF_8.toString()));
        return sb.toString();
    }
    
    private BigInteger postJob(CloseableHttpClient httpClient, JSONObject credentials, File bundle, JsonObject jobConfigOverlay)
            throws ClientProtocolException, IOException {

        
        String url = getSubmitURL(credentials, bundle);
        
        HttpPost postJobWithConfig = new HttpPost(url);
        postJobWithConfig.addHeader("accept", ContentType.APPLICATION_JSON.getMimeType());
	postJobWithConfig.addHeader(AUTH.WWW_AUTH_RESP, getAPIKey(credentials.get("userid").toString(),
						       credentials.get("password").toString()));
        FileBody bundleBody = new FileBody(bundle,
                ContentType.APPLICATION_OCTET_STREAM);
        StringBody configBody = new StringBody(jobConfigOverlay.toString(),
                ContentType.APPLICATION_JSON);

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("sab", bundleBody).addPart(DeployKeys.JOB_CONFIG_OVERLAYS, configBody)
                .build();

        postJobWithConfig.setEntity(reqEntity);

        JSONObject jsonResponse = getJsonResponse(httpClient, postJobWithConfig);
        
        Topology.STREAMS_LOGGER.info("Streaming Analytics Service submit job response:" + jsonResponse.serialize());
        
        Object jobId = jsonResponse.get("jobId");
        if (jobId == null)
            return BigInteger.valueOf(-1);
        return new BigInteger(jobId.toString());
    }
    
    private JsonObject getBluemixSubmitConfig( Map<String, Object> config) throws IOException {
        
        JobConfig jc = JobConfig.fromProperties(config);
        
        // Streaming Analytics service is always using 4.2 or later
        // so use the job config overlay
            
        JobConfigOverlay jco = new JobConfigOverlay(jc);
        
        return jco.fullOverlayAsJSON();
    }
    
    private BigInteger submitJobToService(File bundle, Map<String, Object> config) throws IOException {
        
        final JsonObject serviceg = getVCAPService(config);
        JSONObject service = JSONObject.parse(serviceg.toString()); //temp            

              
        final JSONObject credentials = (JSONObject) service.get("credentials");
        final JsonObject credentialsg = serviceg.getAsJsonObject("credentials");
        
        final CloseableHttpClient httpClient = createHttpClient(credentials);
        try {
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service: Checking status :" + service.get("name"));
            
            RestUtils.checkInstanceStatus(httpClient, credentialsg);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service: Submitting bundle : " + bundle.getName() + " to " + service.get("name"));
            
            JsonObject jcojson = getBluemixSubmitConfig(config);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service submit job request:" + jcojson.toString());

            return postJob(httpClient, credentials, bundle, jcojson);
        } finally {
            httpClient.close();
        }
    }

    static String getAPIKey(String userid, String password) {
        String api_creds = userid + ":" + password;
        String apiKey = "Basic " + DatatypeConverter.printBase64Binary(
                api_creds.getBytes(StandardCharsets.UTF_8));
        return apiKey;
    }
}
