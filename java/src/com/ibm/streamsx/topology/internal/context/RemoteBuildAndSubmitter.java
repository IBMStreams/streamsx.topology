package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;

public class RemoteBuildAndSubmitter{ 

	public Future<BigInteger> submit(Topology app, Map<String, Object> config) throws Exception {
	    Topology.STREAMS_LOGGER.info("Remote Build And Submitter: beginning remote build of application.");
	    StreamsContext<File> zippedSc = new ZippedToolkitStreamsContext();
	    
	    Future<File> archive = zippedSc.submit(app, config);
	    return new CompletedFuture<BigInteger>(doSubmit(config, archive.get()));
	}
	

	public Future<BigInteger> submit(JSONObject submission) throws Exception {
	    Topology.STREAMS_LOGGER.info("Remote Build And Submitter: beginning remote build of application.");
	    StreamsContext<File> zippedSc = new ZippedToolkitStreamsContext();
	    
	    Future<File> archive = zippedSc.submit(submission);
		Map<String, Object> config = Contexts.jsonDeployToMap(
				(JSONObject)submission.get("deploy"));
		return new CompletedFuture<BigInteger>(doSubmit(config, archive.get()));
	}
	
	private BigInteger doSubmit(Map<String, Object> config, File archive) throws IOException{
		JSONObject service = Contexts.getVCAPService(config);
        JSONObject credentials = (JSONObject) service.get("credentials");
     
        BuildServiceRESTWrapper wrapper = new BuildServiceRESTWrapper(credentials);
        return new BigInteger((String)wrapper.remoteBuildAndSubmit(archive).get("jobId"));
	}
}
