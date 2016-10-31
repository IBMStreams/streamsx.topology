/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.context.remote.RemoteContextFactory.getRemoteContext;
import static com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext.makeDirectoryStructure;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities.gson;
import static com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities.json4j;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.streams.InvokeMakeToolkit;

public class ToolkitStreamsContext extends StreamsContextImpl<File> {

	static final Logger trace = Topology.TOPOLOGY_LOGGER;
	
    Map<String, Object> graphItems;
    
    @Override
    public Type getType() {
        return Type.TOOLKIT;
    }

    @Override
    public Future<File> submit(Topology app, Map<String, Object> config)
            throws Exception {

        if (config == null)
            config = new HashMap<>();

        // If the toolkit path is not given, then create one in the
        // currrent directory.
        if (!config.containsKey(ContextProperties.TOOLKIT_DIR)) {
            config.put(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }

        File toolkitRoot = new File((String) config.get(ContextProperties.TOOLKIT_DIR));

        makeDirectoryStructure(toolkitRoot,
                (String) app.builder().json().get("namespace"));
        
        graphItems = app.finalizeGraph(getType(), config);
        
        addConfigToJSON(app.builder().getConfig(), config);
        
        JSONObject jsonGraph = app.builder().complete();
        
        JSONObject deploy = new JSONObject();
        deploy.put(ContextProperties.TOOLKIT_DIR, toolkitRoot.getAbsolutePath());
        if (config.containsKey(ContextProperties.KEEP_ARTIFACTS))
            deploy.put(KEEP_ARTIFACTS, config.get(KEEP_ARTIFACTS));
        
        JSONObject submission = new JSONObject();
        submission.put(SUBMISSION_DEPLOY, deploy);
        submission.put(SUBMISSION_GRAPH, jsonGraph);
        
        return createToolkit(submission);
    }
    
    @Override
    public Future<File> submit(JSONObject submission) throws Exception {
        return createToolkit(submission);
    }
    
    private Future<File> createToolkit(JSONObject submission) throws Exception {
        
        // use the remote context to build the toolkit.
        @SuppressWarnings("unchecked")
        RemoteContext<File> tkrc = (RemoteContext<File>) getRemoteContext(RemoteContext.Type.TOOLKIT);
        
        JsonObject gsonSubmission = gson(submission);
        final Future<File> future = tkrc.submit(gsonSubmission);
        final File toolkitRoot = future.get();
        
        JsonObject gsonDeploy = object(gsonSubmission, SUBMISSION_DEPLOY);
        
        // Patch up the returned deploy info.
        JSONObject deployInfo = json4j(gsonDeploy);
        submission.put(SUBMISSION_DEPLOY, deployInfo);
        
        // Index the toolkit
        makeToolkit(deployInfo, toolkitRoot);
        
        return future;
    }
    
    protected void makeToolkit(JSONObject deployInfo, File toolkitRoot) throws InterruptedException, Exception{
        // Invoke spl-make-toolkit 
        InvokeMakeToolkit imt = new InvokeMakeToolkit(deployInfo, toolkitRoot);
        imt.invoke();
    }

    protected void addConfigToJSON(JSONObject graphConfig, Map<String,Object> config) {
        
        for (String key : config.keySet()) {
            Object value = config.get(key);
            
            if (key.equals(ContextProperties.SUBMISSION_PARAMS)) {
                // value causes issues below and no need to add this to json
                continue;
            }
            if (JSONObject.isValidObject(value)) {
                graphConfig.put(key, value);
                continue;
            }
            if (value instanceof Collection) {
                JSONArray ja = new JSONArray();
                @SuppressWarnings("unchecked")
                Collection<Object> coll = (Collection<Object>) value;
                ja.addAll(coll);
                graphConfig.put(key, ja);            
            }
        }
    }
}
