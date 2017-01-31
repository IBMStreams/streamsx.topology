/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.ContextProperties.TOOLKIT_DIR;
import static com.ibm.streamsx.topology.context.remote.RemoteContextFactory.getRemoteContext;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext.makeDirectoryStructure;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
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
    
    @Override
    public Type getType() {
        return Type.TOOLKIT;
    }

    @Override
    Future<File> _submit(Topology app, Map<String, Object> config)
            throws Exception {
        
        if (false) {

        // If the toolkit path is not given, then create one in the
        // currrent directory.
        if (!config.containsKey(ContextProperties.TOOLKIT_DIR)) {
            config.put(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }

        File toolkitRoot = new File((String) config.get(ContextProperties.TOOLKIT_DIR));

        makeDirectoryStructure(toolkitRoot,
                (String) app.builder().json().get("namespace"));
        }
        
        addConfigToJSON(app.builder().getConfig(), config);       
        
        JsonObject submission = createSubmission(app, config);
        // deploy(submission).addProperty(TOOLKIT_DIR, toolkitRoot.getAbsolutePath());
        
        return _submit(submission);
    }
    
    @Override
    Future<File> _submit(JsonObject submission) throws Exception {
        return createToolkit(submission);
    }
    
    private Future<File> createToolkit(JsonObject submission) throws Exception {
        
        // use the remote context to build the toolkit.
        @SuppressWarnings("unchecked")
        RemoteContext<File> tkrc = (RemoteContext<File>) getRemoteContext(RemoteContext.Type.TOOLKIT);
        
        final Future<File> future = tkrc.submit(submission);
        final File toolkitRoot = future.get();
        
        JsonObject deploy = object(submission, SUBMISSION_DEPLOY);
                
        // Index the toolkit
        makeToolkit(deploy, toolkitRoot);
        
        return future;
    }
    
    protected void makeToolkit(JsonObject deploy, File toolkitRoot) throws InterruptedException, Exception{
        // Invoke spl-make-toolkit 
        InvokeMakeToolkit imt = new InvokeMakeToolkit(deploy, toolkitRoot);
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
