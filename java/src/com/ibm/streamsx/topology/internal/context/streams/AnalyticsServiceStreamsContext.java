/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext.streamingAnalyticServiceFromDeploy;
import static com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices.getVCAPService;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.ActiveVersion;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.context.remote.RemoteContexts;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.context.service.RemoteStreamingAnalyticsServiceStreamsContext;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

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
    protected Future<BigInteger> action(AppEntity entity) throws Exception {
        if (useRemoteBuild(entity, AnalyticsServiceStreamsContext::getSasServiceBase)) {
            RemoteStreamingAnalyticsServiceStreamsContext rc = new RemoteStreamingAnalyticsServiceStreamsContext();
            return rc.submit(entity.submission);
        }

        return super.action(entity);
    }
    
    /**
     * Get the SAS OS version base.
     */
    private static int getSasServiceBase(AppEntity entity) {
                 
            // Need to interrogate the service to figure out
            // the os version of the service.
            StreamingAnalyticsService sas;
            try {
                sas = sas(entity);
                Instance instance = sas.getInstance();
                ActiveVersion ver = instance.getActiveVersion();
                
                // Compare base versions, ir it doesn't exactly match the
                // service force remote build.
                return Integer.valueOf(ver.getMinimumOSBaseVersion());

            } catch (IOException e) {
                ;
            }

            return -1;
    }
    
    
    @Override
    Future<BigInteger> invoke(AppEntity entity, File bundle) throws Exception {
        try {           
            BigInteger jobId = submitJobToService(bundle, entity);
         
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
    
    /**
     * Get the connection to the service and check it is running.
     */
    private static StreamingAnalyticsService sas(AppEntity entity) throws IOException {
        
        StreamingAnalyticsService sas = (StreamingAnalyticsService)
                entity.getSavedObject(StreamingAnalyticsService.class);
        
        if (sas == null) {
            JsonObject deploy = deploy(entity.submission);
            sas = entity.saveObject(StreamingAnalyticsService.class, streamingAnalyticServiceFromDeploy(deploy));
            RemoteContexts.checkServiceRunning(sas);
        }
        
        return sas;
    }

    private BigInteger submitJobToService(File bundle, AppEntity entity) throws IOException {
        final JsonObject submission = entity.submission;
        JsonObject deploy =  deploy(submission);
        JsonObject jco = DeployKeys.copyJobConfigOverlays(deploy);

        final StreamingAnalyticsService sas = sas(entity); 

        Result<Job, JsonObject> submitResult = sas.submitJob(bundle, jco);
        final JsonObject submissionResult = GsonUtilities.objectCreate(submission,
                RemoteContext.SUBMISSION_RESULTS);
        GsonUtilities.addAll(submissionResult, submitResult.getRawResult());
        
        // Ensure job id is in a known place regardless of version
        final String jobId = submitResult.getId();
        GsonUtilities.addToObject(submissionResult, SubmissionResultsKeys.JOB_ID, jobId);

        return new BigInteger(jobId);
    }
}
