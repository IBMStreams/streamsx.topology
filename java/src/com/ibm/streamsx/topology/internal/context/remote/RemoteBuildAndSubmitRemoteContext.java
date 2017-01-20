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
	public Future<File> submit(JsonObject submission) throws Exception {
		Future<File> archive = super.submit(submission);
		JsonObject deploy = object(submission, "deploy");
		doSubmit(deploy, archive.get());
       return archive;
	}
	
	private void doSubmit(JsonObject deploy, File archive) throws IOException{
		JsonObject service = VcapServices.getVCAPService(key -> deploy.get(key));        
        JsonObject credentials = object(service,  "credentials");
     
        BuildServiceRemoteRESTWrapper wrapper = new BuildServiceRemoteRESTWrapper(credentials);
        wrapper.remoteBuildAndSubmit(deploy, archive);
	}
}
