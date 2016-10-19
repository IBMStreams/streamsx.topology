package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;

public class RemoteBuildAndSubmitStreamsContext extends ZippedToolkitStreamsContext {
	@Override
    public Type getType() {
        return Type.REMOTE_BUILD_AND_SUBMIT;
    }
	
	@Override
	public Future<File> submit(Topology app, Map<String, Object> config) throws Exception { 
		Future<File> archive = super.submit(app,  config);
		doSubmit(config, archive.get());
        return archive;
	}
	
	@Override
	public Future<File> submit(JSONObject submission) throws Exception {
		Future<File> archive = super.submit(submission);
		Map<String, Object> config = Contexts.jsonDeployToMap(
				(JSONObject)submission.get("deploy"));
		doSubmit(config, archive.get());
       return archive;
	}
	
	private void doSubmit(Map<String, Object> config, File archive) throws IOException{
		JSONObject service = Contexts.getVCAPService(config);        
        JSONObject credentials = (JSONObject) service.get("credentials");
     
        BuildServiceRESTWrapper wrapper = new BuildServiceRESTWrapper(credentials);
        wrapper.remoteBuildAndSubmit(archive);
	}
}
