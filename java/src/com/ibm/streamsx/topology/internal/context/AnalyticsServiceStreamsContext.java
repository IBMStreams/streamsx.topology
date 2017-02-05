/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices.getVCAPService;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.Future;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streaminganalytics.RestUtils;

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
    Future<BigInteger> invoke(AppEntity entity, File bundle) throws Exception {
        try {
            JsonObject deploy =  deploy(entity.submission);
            
            BigInteger jobId = submitJobToService(bundle, deploy);

            return new CompletedFuture<BigInteger>(jobId);
        } finally {
            if (!keepArtifacts(entity.submission))
                bundle.delete();
        }
    }
    
    /**
     * Verify we have a valid Streaming Analytic service
     * information before we attempt anything.
     */
    @Override
    void preSubmit(AppEntity entity) {
        
            
        try {
            if (entity.submission != null)
                getVCAPService(deploy(entity.submission));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    } 
    
    /*
   
    private JsonObject getBluemixSubmitConfig( Map<String, Object> config) throws IOException {
        
        JobConfig jc = JobConfig.fromProperties(config);
        
        // Streaming Analytics service is always using 4.2 or later
        // so use the job config overlay
            
        JobConfigOverlay jco = new JobConfigOverlay(jc);
        
        return jco.fullOverlayAsJSON(new JsonObject());
    }
    */
    
    private BigInteger submitJobToService(File bundle, JsonObject deploy) throws IOException {
        
        final JsonObject service = getVCAPService(deploy);
        final String serviceName = jstring(service, "name");
              
        final JsonObject credentials = service.getAsJsonObject("credentials");
        
        final CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service: Checking status :" + serviceName);
            
            RestUtils.checkInstanceStatus(httpClient, credentials);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service: Submitting bundle : " + bundle.getName() + " to " + serviceName);
            
            JsonObject jcojson = DeployKeys.copyJobConfigOverlays(deploy);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics Service submit job request:" + jcojson.toString());

            return RestUtils.postJob(httpClient, credentials, bundle, jcojson);
        } finally {
            httpClient.close();
        }
    }
}
