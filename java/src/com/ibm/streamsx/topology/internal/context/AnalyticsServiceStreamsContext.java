/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.AnalyticsServiceProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities;
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
    Future<BigInteger> _submit(Topology app, Map<String, Object> config)
            throws Exception {

        preBundle(config);
        File bundle = bundler._submit(app, config).get();

        preInvoke();

        BigInteger jobId = submitJobToService(bundle, config);
        
        return new CompletedFuture<BigInteger>(jobId);
    }

    @Override
    Future<BigInteger> _submit(JsonObject submission) throws Exception {
        
        JSONObject _submission = JSON4JUtilities.json4j(submission);
        
        Map<String, Object> config = Contexts
                .jsonDeployToMap((JSONObject) _submission.get(RemoteContext.SUBMISSION_DEPLOY));

        preBundle(config);
        File bundle = bundler._submit(submission).get();
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
   
    private JsonObject getBluemixSubmitConfig( Map<String, Object> config) throws IOException {
        
        JobConfig jc = JobConfig.fromProperties(config);
        
        // Streaming Analytics service is always using 4.2 or later
        // so use the job config overlay
            
        JobConfigOverlay jco = new JobConfigOverlay(jc);
        
        return jco.fullOverlayAsJSON(new JsonObject());
    }
    
    private BigInteger submitJobToService(File bundle, Map<String, Object> config) throws IOException {
        
        final JsonObject service = getVCAPService(config);
        final String serviceName = jstring(service, "name");
              
        final JsonObject credentials = service.getAsJsonObject("credentials");
        
        final CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service: Checking status :" + serviceName);
            
            RestUtils.checkInstanceStatus(httpClient, credentials);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service: Submitting bundle : " + bundle.getName() + " to " + serviceName);
            
            JsonObject jcojson = getBluemixSubmitConfig(config);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service submit job request:" + jcojson.toString());

            return RestUtils.postJob(httpClient, credentials, bundle, jcojson);
        } finally {
            httpClient.close();
        }
    }
}
