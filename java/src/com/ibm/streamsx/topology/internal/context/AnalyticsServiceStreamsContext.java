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
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streaminganalytics.RestUtils;

public class AnalyticsServiceStreamsContext extends
        BundleUserStreamsContext<BigInteger> {

    private final Type type;
    
    public AnalyticsServiceStreamsContext(Type type) {
        super(false);
        this.type = type;
    }

    @Override
    public Type getType() {
        return type;
    }
    
    @Override
    Future<BigInteger> invoke(AppEntity entity, File bundle) throws Exception {
        try {           
            BigInteger jobId = submitJobToService(bundle, entity.submission);
         
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
    
    private BigInteger submitJobToService(File bundle, JsonObject submission) throws IOException {
        JsonObject deploy =  deploy(submission);
        
        final JsonObject service = getVCAPService(deploy);
        final String serviceName = jstring(service, "name");
              
        final JsonObject credentials = service.getAsJsonObject("credentials");
        
        final CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            Topology.STREAMS_LOGGER.info("Streaming Analytics service (" + serviceName + "): Checking status :" + serviceName);
            
            RestUtils.checkInstanceStatus(httpClient, service);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics service (" + serviceName + "): Submitting bundle : " + bundle.getName() + " to " + serviceName);
            
            JsonObject jcojson = DeployKeys.copyJobConfigOverlays(deploy);
            
            Topology.STREAMS_LOGGER.info("Streaming Analytics service (" + serviceName + "): submit job request:" + jcojson.toString());

            JsonObject response = RestUtils.postJob(httpClient, service, bundle, jcojson);
            
            final JsonObject submissionResult = GsonUtilities.objectCreate(submission, RemoteContext.SUBMISSION_RESULTS);
            GsonUtilities.addAll(submissionResult, response);
            
            String jobId = jstring(response, "jobId");
            
            if (jobId == null)
                return BigInteger.valueOf(-1);
            
            return new BigInteger(jobId);
        } finally {
            httpClient.close();
        }
    }
}
