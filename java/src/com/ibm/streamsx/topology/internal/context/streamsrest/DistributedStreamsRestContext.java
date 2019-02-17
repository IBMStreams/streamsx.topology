package com.ibm.streamsx.topology.internal.context.streamsrest;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map.Entry;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.rest.ApplicationBundle;
import com.ibm.streamsx.rest.Instance;
import com.ibm.streamsx.rest.Job;
import com.ibm.streamsx.rest.Result;
import com.ibm.streamsx.rest.StreamsConnection;
import com.ibm.streamsx.rest.build.BuildService;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

/**
 * Distributed context that uses the REST api for building
 * jobs and submission.
 */
public class DistributedStreamsRestContext extends BuildServiceContext {
    
    @Override
    public Type getType() {
        return Type.DISTRIBUTED;
    }
    
    @Override
    protected BuildService createSubmissionContext(JsonObject deploy) throws Exception {
        BuildService builder = super.createSubmissionContext(deploy);
        
        // Verify the Streams service endpoint has the correct format.
        StreamsKeys.getStreamsInstanceURL(deploy);
        
        return builder;
    }
    
    @Override
    protected void postBuildAction(JsonObject deploy, JsonObject jco, JsonObject result) throws Exception {
        
        URL instanceUrl  = new URL(StreamsKeys.getStreamsInstanceURL(deploy));
        
        String path = instanceUrl.getPath();
        
        String instanceId = path.substring("/streams/rest/instances/".length());
        if (instanceId.endsWith("/"))
            instanceId = instanceId.substring(0, instanceId.length()-1);
        
        instanceId = URLDecoder.decode(instanceId, StandardCharsets.UTF_8.name());
        
        URL restUrl = new URL(instanceUrl.getProtocol(), instanceUrl.getHost(), instanceUrl.getPort(),
                "/streams/rest/resources");
                       
        StreamsConnection conn = StreamsConnection.ofBearerToken(restUrl.toExternalForm(), StreamsKeys.getBearerToken(deploy));
        
        if (!sslVerify(deploy))
            conn.allowInsecureHosts(true);
                
        Instance instance = conn.getInstance(instanceId);
        
        JsonArray artifacts = GsonUtilities.array(GsonUtilities.object(result, "build"), "artifacts");
        try {
            if (artifacts == null || artifacts.size() != 1)
                throw new IllegalStateException();

            String location = GsonUtilities
                    .jstring(artifacts.get(0).getAsJsonObject(), "location");

            report("Uploading bundle");
            ApplicationBundle bundle = instance
                    .uploadBundle(new File(location));

            report("Submitting job");
            Result<Job, JsonObject> submissionResult = bundle.submitJob(jco);

            for (Entry<String, JsonElement> entry : submissionResult
                    .getRawResult().entrySet())
                result.add(entry.getKey(), entry.getValue());
            
            report("Job id:" + submissionResult.getId());
        } finally {
            if (!jboolean(deploy, KEEP_ARTIFACTS)) {
                for (JsonElement e : artifacts) {
                    JsonObject artifact = e.getAsJsonObject();
                    if (artifact.has("location")) {
                        new File(GsonUtilities.jstring(artifact, "location")).delete();
                    }
                }
            }
        }
    }
    
}
