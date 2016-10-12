package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.Future;

import javax.xml.bind.DatatypeConverter;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

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
        return null;
	}
	
	@Override
	public Future<File> submit(JSONObject submission) throws Exception {
		Future<File> archive = super.submit(submission);
		Map<String, Object> config = RemoteContexts.jsonDeployToMap(
				(JSONObject)submission.get("deploy"));
		doSubmit(config, archive.get());
       return null;
	}
	
	private void doSubmit(Map<String, Object> config, File archive) throws IOException{
		JSONObject service = RemoteContexts.getVCAPService(config);        
        JSONObject credentials = (JSONObject) service.get("credentials");
     
        BuildServiceRESTWrapper wrapper = new BuildServiceRESTWrapper(credentials);
        wrapper.remoteBuildAndSubmit(archive);
	}
}
