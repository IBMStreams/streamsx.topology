package com.ibm.streamsx.topology.internal.context.remote;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public class RemoteBuildAndSubmitRemoteContext extends ZippedToolkitRemoteContext {
	@Override
    public Type getType() {
        return Type.REMOTE_BUILD_AND_SUBMIT;
    }
	
	@Override
	public Future<File> submit(JsonObject submission) throws Exception {
		Future<File> archive = super.submit(submission);
		Map<String, Object> config = RemoteContexts.gsonDeployToMap(
				GsonUtilities.object(submission, "deploy"));
		doSubmit(config, archive.get());
       return archive;
	}
	
	private void doSubmit(Map<String, Object> config, File archive) throws IOException{
		JsonObject service = RemoteContexts.getVCAPService(config);        
        JsonObject credentials = GsonUtilities.object(service,  "credentials");
     
        BuildServiceRemoteRESTWrapper wrapper = new BuildServiceRemoteRESTWrapper(credentials);
        wrapper.remoteBuildAndSubmit(archive);
	}
}
