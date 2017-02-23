/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.streaminganalytics.VcapServices;

public class RemoteBuildAndSubmitRemoteContext extends ZippedToolkitRemoteContext {
	@Override
    public Type getType() {
        return Type.ANALYTICS_SERVICE;
    }
	
	@Override
	public Future<File> _submit(JsonObject submission) throws Exception {
	    preSubmit(submission);
	    // Get the VCAP service info which also verifies we have the
	    // right information before we do any work.
	    JsonObject deploy = object(submission, "deploy");
	    JsonObject service = VcapServices.getVCAPService(deploy);
	    
	    Future<File> archive = super._submit(submission);
		
		doSubmit(submission, service, archive.get());
		return postSubmit(submission, archive);
	}
	
	private void doSubmit(JsonObject submission, JsonObject service, File archive) throws IOException{
        BuildServiceRemoteRESTWrapper wrapper = new BuildServiceRemoteRESTWrapper(service);
        wrapper.remoteBuildAndSubmit(submission, archive);
	}
}
