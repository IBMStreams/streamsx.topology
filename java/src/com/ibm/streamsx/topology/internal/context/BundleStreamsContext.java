/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext.deleteToolkit;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.core.InternalProperties;
import com.ibm.streamsx.topology.internal.graph.GraphKeys;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeSc;

public class BundleStreamsContext extends ToolkitStreamsContext {

    static final Logger trace = Topology.TOPOLOGY_LOGGER;

    private final boolean standalone;

    public BundleStreamsContext(boolean standalone) {
        this.standalone = standalone;
    }

    @Override
    public Type getType() {
        return standalone ? Type.STANDALONE_BUNDLE : Type.BUNDLE;
    }

    @Override
    Future<File> _submit(Topology app, Map<String, Object> config)
            throws Exception {

        File appDir;
        if (!config.containsKey(ContextProperties.APP_DIR)) {
            appDir = Files.createTempDirectory("apptk").toFile();
            config.put(ContextProperties.APP_DIR, appDir.getAbsolutePath());

        } else {
            appDir = new File((String) (config.get(ContextProperties.APP_DIR)));
        }
        config.put(ContextProperties.TOOLKIT_DIR, appDir.getAbsolutePath());

        File appDirA = super._submit(app, config).get();
                
        JsonObject submission = createSubmission(app, config);
        
        return doSPLCompile(appDirA, submission);
    }
    
    @Override
    Future<File> _submit(JsonObject submission) throws Exception {
    	
    	File appDir = super._submit(submission).get();
    	return doSPLCompile(appDir, submission);
    }
    
    private Future<File> doSPLCompile(File appDir, JsonObject submission) throws Exception {
    	 	
    	JsonObject deploy = deploy(submission);
    	JsonObject graph = GraphKeys.graph(submission);
    	
        String namespace = jstring(graph, "namespace");
        String name = jstring(graph, "name");

        InvokeSc sc = new InvokeSc(deploy, standalone, namespace, name, appDir);
        
        // Add the toolkits
        JsonObject splConfig = object(graph, "config", "spl");
        if (splConfig != null) {
            JsonArray toolkits = array(splConfig, InternalProperties.TOOLKITS_JSON);
            
            for (JsonElement obj : toolkits) {
                JsonObject tkinfo = obj.getAsJsonObject();
                String root = jstring(tkinfo, "root");
                if (root != null)
                    sc.addToolkit(new File(root));
            }
        }

        sc.invoke();

        File outputDir = new File(appDir, "output");
        String bundleName = namespace + "." + name + ".sab";
        File bundle = new File(outputDir, bundleName);

        File localBundle = new File(bundle.getName());

        Files.copy(bundle.toPath(), localBundle.toPath(),
                StandardCopyOption.REPLACE_EXISTING);

        if (!deleteToolkit(appDir, deploy))
            trace.info("Keeping toolkit at: " + appDir.getAbsolutePath());

        trace.info("Streams Application Bundle produced: "
                + localBundle.getName());

        return new CompletedFuture<File>(localBundle);
    }  
}
