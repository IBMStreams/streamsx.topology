/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
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
	    JsonObject service = VcapServices.getVCAPService(deploy);
	    
	    Future<File> archive = super._submit(submission);
	    
	    File buildArchive =  archive.get();
		
	    try {
		    doSubmit(submission, service, buildArchive);
	    } finally {		
		    if (!keepArtifacts(submission))
		        buildArchive.delete();
	    }
		
		return archive;
	}
	
	private void doSubmit(JsonObject submission, JsonObject service, File archive) throws IOException{
        BuildServiceRemoteRESTWrapper wrapper = new BuildServiceRemoteRESTWrapper(service);
        wrapper.remoteBuildAndSubmit(submission, archive);
	}
}
