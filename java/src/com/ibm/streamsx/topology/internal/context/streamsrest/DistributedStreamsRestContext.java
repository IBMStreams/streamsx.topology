package com.ibm.streamsx.topology.internal.context.streamsrest;

import java.io.File;
import java.net.URL;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.ApplicationBundle;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public class DistributedStreamsRestContext extends BuildServiceContext {
    
    @Override
    protected void postBuildAction(JsonObject deploy, JsonObject jco, JsonObject result) throws Exception {
        
        URL instanceUrl  = new URL(StreamsKeys.getStreamsInstanceURL(deploy));
        
        String path = instanceUrl.getPath();
        
        String instanceId = path.substring("/streams/rest/instances/".length());
        if (instanceId.endsWith("/"))
            instanceId = instanceId.substring(0, instanceId.length()-1);
        
        URL restUrl = new URL(instanceUrl.getProtocol(), instanceUrl.getHost(), instanceUrl.getPort(), "/streams/rest/resources");
                       
        StreamsConnection conn = StreamsConnection.ofBearerToken(restUrl.toExternalForm(), StreamsKeys.getBearerToken(deploy));
        
        conn.allowInsecureHosts(true);
        
        System.err.println("REST_URL:" + restUrl.toExternalForm());
        
        Instance instance = conn.getInstance(instanceId);
        
        JsonArray artifacts = GsonUtilities.array(GsonUtilities.object(result, "build"), "artifacts");
        if (artifacts == null || artifacts.size() != 1)
            throw new IllegalStateException();
        
        String location = GsonUtilities.jstring(artifacts.get(0).getAsJsonObject(), "location");
        
        ApplicationBundle bundle = instance.uploadBundle(new File(location));
                       
        Result<Job, JsonObject> submissionResult = bundle.submitJob(jco);
        
        System.out.println("Submission Result:" + submissionResult.getRawResult());
    }
    
}
