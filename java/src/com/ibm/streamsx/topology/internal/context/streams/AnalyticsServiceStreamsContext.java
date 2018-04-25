/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.context.ContextProperties.FORCE_REMOTE_BUILD;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.context.remote.RemoteBuildAndSubmitRemoteContext.streamingAnalyticServiceFromDeploy;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices.getVCAPService;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.math.BigInteger;
import java.nio.file.Files;
import java.util.List;
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
        if (useRemoteBuild(entity)) {
            RemoteStreamingAnalyticsServiceStreamsContext rc = new RemoteStreamingAnalyticsServiceStreamsContext();
            return rc.submit(entity.submission);
        }

        return super.action(entity);
    }
    
    /**
     * See if the remote build service should be used.
     * If STREAMS_INSTALL was not set is handled elsewhere,
     * so this path assumes that STREAMS_INSTALL is set and not empty.
     * 
     * Remote build if:
     *  - FORCE_REMOTE_BUILD is set to true.
     *  - Architecture cannot be determined as x86_64. (assumption that service is always x86_64)
     *  - OS is not Linux.
     *  - OS is not centos or redhat
     *  - OS is less than version 6.
     *  - OS service base does not match local os version
     */
    private boolean useRemoteBuild(AppEntity entity) {
        
        assert System.getenv("STREAMS_INSTALL") != null;
        assert !System.getenv("STREAMS_INSTALL").isEmpty();
        
        final JsonObject deploy = deploy(entity.submission);        
        if (jboolean(deploy, FORCE_REMOTE_BUILD))
            return true;
        
        try {
            final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            
            final String arch = os.getArch();
            boolean x86_64 = "x86_64".equalsIgnoreCase(arch) || "amd64".equalsIgnoreCase(arch);
            if (!x86_64)
                return true;
            
            if (!"Linux".equalsIgnoreCase(os.getName()))
                return true; 
                        
            // /etc/system-release-cpe examples
            //
            // cpe:/o:centos:linux:6:GA
            // cpe:/o/redhat:enterprise_linux:7.2:ga:server
           
            File cpeFile = new File("/etc/system-release-cpe");
            if (!cpeFile.isFile())
                return true;
            List<String> lines = Files.readAllLines(cpeFile.toPath());
            
            String osVendor = null;
            String osVersion = null;
            String osUpdate = null;
            for (String cpe : lines) {
                cpe = cpe.trim();
                if (!cpe.startsWith("cpe:"))
                    continue;
                String[] comps = cpe.split(":");
                if (comps.length < 6)
                    continue;
                if (!"cpe".equals(comps[0]))
                    continue;  
                if (!"/o".equals(comps[1]))
                    continue;
                
                if (!comps[2].isEmpty()) {
                    osVendor = comps[2];
                    osVersion = comps[4];
                    osUpdate = comps[5];
                }
            }
            
            // If we can' determine the info, force remote
            if (osVendor == null || osVendor.isEmpty())
                return true;
            if (osVersion == null || osVersion.isEmpty())
                return true;
            if (osUpdate == null || osUpdate.isEmpty())
                return true;
            
            if (!"centos".equals(osVendor) && !"redhat".equals(osVendor))
                return true;

            double version = Double.valueOf(osVersion);
            if (version < 6)
                return true;
            
            // Need to interrogate the service to figure out
            // the os version of the service.
            StreamingAnalyticsService sas = sas(entity);
            Instance instance = sas.getInstance();
            ActiveVersion ver = instance.getActiveVersion();
            
            // Compare base versions, ir it doesn't exactly match the
            // service force remote build.
            int serviceBase = Integer.valueOf(ver.getMinimumOSBaseVersion());
            if (((int) version) != serviceBase)
                return true;          
            
        } catch (SecurityException | IOException | NumberFormatException e) {
            // Can't determine information, force remote.
            return true;
        }
     
        return false;
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
    private StreamingAnalyticsService sas(AppEntity entity) throws IOException {
        
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
