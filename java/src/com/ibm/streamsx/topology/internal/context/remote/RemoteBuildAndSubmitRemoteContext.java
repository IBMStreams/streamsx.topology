/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2019
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_DEFINITION;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.SERVICE_NO_CHECK_PERIOD;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.SERVICE_RUNNING_TIME;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

/**
 * Build and job submitter using a build archive and the build service
 * for the Streaming Analytics service on public cloud.
 */
public class RemoteBuildAndSubmitRemoteContext extends BuildRemoteContext<StreamingAnalyticsService> {
    
    private static final ThreadLocal<Map<String,Long>> SERVICE_ACCESS = new ThreadLocal<Map<String,Long>>() {
        protected java.util.Map<String,Long> initialValue() {
            return new HashMap<>();
        }
    };
    
    
	@Override
    public Type getType() {
        return Type.STREAMING_ANALYTICS_SERVICE;
    }
	
    @Override
    protected StreamingAnalyticsService createSubmissionContext(JsonObject deploy)
            throws Exception {
        return streamingAnalyticServiceFromDeploy(deploy);
    }
	
    @Override
    protected JsonObject submitBuildArchive(StreamingAnalyticsService sas,
            File buildArchive, JsonObject deploy, JsonObject jco,
            String buildName, JsonObject buildConfig) throws Exception {

        // See if we can avoid a service check from
        // a time in the JSON deploy section or the last
        // time this thread saw the service was running.
        boolean checkIfRunning = true;
        if (deploy.has(SERVICE_RUNNING_TIME)) {
            long last = deploy.get(SERVICE_RUNNING_TIME).getAsLong();
            checkIfRunning = (System.currentTimeMillis()
                    - last) > SERVICE_NO_CHECK_PERIOD;
        } else if (SERVICE_ACCESS.get().containsKey(sas.getName())) {
            long last = SERVICE_ACCESS.get().get(sas.getName());
            checkIfRunning = (System.currentTimeMillis()
                    - last) > SERVICE_NO_CHECK_PERIOD;
        }

        if (checkIfRunning) {
            report("Checking service");
            RemoteContexts.checkServiceRunning(sas);
        }

        report("Building & submitting job");
        Result<Job, JsonObject> submitResult = sas
                .buildAndSubmitJob(buildArchive, jco, buildName, buildConfig);

        SERVICE_ACCESS.get().put(sas.getName(), System.currentTimeMillis());

        // Ensure job id is in a known place regardless of version
        final String jobId = submitResult.getId();
        submitResult.getRawResult().addProperty(SubmissionResultsKeys.JOB_ID,
                jobId);
        
        report("Job id:" + jobId);

        return submitResult.getRawResult();
    }

	/**
	 * Get the StreamingAnalyticsService from deploy section of graph.
	 * Used consistently when we need one for topology.
	 */
    public static StreamingAnalyticsService streamingAnalyticServiceFromDeploy(JsonObject deploy) throws IOException {
        final StreamingAnalyticsService sas;
        if (deploy.has(SERVICE_DEFINITION)) {
            sas = StreamingAnalyticsService.of(object(deploy, SERVICE_DEFINITION));
        } else {
            JsonObject vcapServices = VcapServices.getVCAPServices(deploy.get(VCAP_SERVICES));
            sas = StreamingAnalyticsService.of(vcapServices,
                    jstring(deploy, SERVICE_NAME));
        }
        return sas;
    }
}
