/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices.getVCAPService;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

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
    protected void preSubmit(AppEntity entity) {
        
            
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
        JsonObject jco = DeployKeys.copyJobConfigOverlays(deploy);

        JsonObject vcapServices = VcapServices.getVCAPServices(deploy.get(VCAP_SERVICES));

        System.err.println("CONTEXT:" + vcapServices);
        System.err.println("CONTEXT:NAME:" + jstring(deploy, SERVICE_NAME));

        final StreamingAnalyticsService sas = StreamingAnalyticsService.of(vcapServices,
                jstring(deploy, SERVICE_NAME));

        Job job = sas.submitJob(bundle, jco);
        final JsonObject result = GsonUtilities.objectCreate(submission,
                RemoteContext.SUBMISSION_RESULTS);

        if (null == job) {
            return BigInteger.valueOf(-1);
        }

        String jobId = job.getId();
        GsonUtilities.addToObject(result, SubmissionResultsKeys.JOB_ID, jobId);

        return new BigInteger(jobId);
    }
}
