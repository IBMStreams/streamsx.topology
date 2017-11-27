/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.SERVICE_NAME;
import static com.ibm.streamsx.topology.context.AnalyticsServiceProperties.VCAP_SERVICES;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.StreamingAnalyticsService;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.graph.GraphKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

public class RemoteBuildAndSubmitRemoteContext extends ZippedToolkitRemoteContext {
	@Override
    public Type getType() {
        return Type.STREAMING_ANALYTICS_SERVICE;
    }
	
	@Override
	public Future<File> _submit(JsonObject submission) throws Exception {
	    // Get the VCAP service info which also verifies we have the
	    // right information before we do any work.
	    JsonObject deploy = deploy(submission);
        JsonObject vcapServices = VcapServices.getVCAPServices(deploy.get(VCAP_SERVICES));
        JsonObject graph = object(submission, "graph");
        // Use the SPL compatible form of the name to ensure
        // that any strange characters in the name provided by
        // the user are not rejected by the build service.
        String buildName = GraphKeys.splAppName(graph);
        JsonObject jco = DeployKeys.copyJobConfigOverlays(deploy);

        final StreamingAnalyticsService sas = StreamingAnalyticsService.of(vcapServices,
                jstring(deploy, SERVICE_NAME));
	    
	    Future<File> archive = super._submit(submission);
	    
	    File buildArchive =  archive.get();
		
	    try {
	        Job job = sas.buildAndSubmitJob(buildArchive, jco, buildName);
	        final JsonObject result = GsonUtilities.objectCreate(submission,
	                RemoteContext.SUBMISSION_RESULTS);

	        if (null != job) {
	            String jobId = job.getId();
	            GsonUtilities.addToObject(result, SubmissionResultsKeys.JOB_ID, jobId);
	        }
	    } finally {		
		    if (!keepArtifacts(submission))
		        buildArchive.delete();
	    }
		
		return archive;
	}
}
