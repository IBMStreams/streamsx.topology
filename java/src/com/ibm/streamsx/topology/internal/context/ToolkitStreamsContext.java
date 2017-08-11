/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.remote.RemoteContextFactory.getRemoteContext;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_STREAMS_VERSION;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.version.Product;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.graph.GraphKeys;
import com.ibm.streamsx.topology.internal.streams.InvokeMakeToolkit;

/**
 * Creates the application toolkit structure and contents.
 * Including building the toolkit using spl-make-toolkit which
 * can be overriden by a sub-class.
 *
 */
public class ToolkitStreamsContext extends JSONStreamsContext<File> {

	static final Logger trace = Topology.TOPOLOGY_LOGGER;
    
	private final boolean keepToolkit;

	public ToolkitStreamsContext() {
        this.keepToolkit = false;
    }
	
    public ToolkitStreamsContext(boolean keepToolkit) {
        this.keepToolkit = keepToolkit;
    }
	
    @Override
    public Type getType() {
        return Type.TOOLKIT;
    }
    
    @Override
    protected Future<File> action(AppEntity entity) throws Exception {
        
        JsonObject submission = entity.submission;
        
        // If no version has been supplied use the current version.
        JsonObject graphConfig = GraphKeys.graphConfig(submission);
        if (!graphConfig.has(CFG_STREAMS_VERSION)) {
            graphConfig.addProperty(CFG_STREAMS_VERSION, Product.getVersion().toString());
        }
        
        // use the remote context to build the toolkit.
        @SuppressWarnings("unchecked")
        RemoteContext<File> tkrc = (RemoteContext<File>) getRemoteContext(RemoteContext.Type.TOOLKIT, keepToolkit);
        
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
