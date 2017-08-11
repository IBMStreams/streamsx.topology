/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context.streams;

import static com.ibm.streamsx.topology.context.ContextProperties.APP_DIR;
import static com.ibm.streamsx.topology.context.ContextProperties.TOOLKIT_DIR;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.createJobConfigOverlayFile;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext.deleteToolkit;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppName;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppNamespace;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.context.ToolkitStreamsContext;
import com.ibm.streamsx.topology.internal.context.remote.SubmissionResultsKeys;
import com.ibm.streamsx.topology.internal.core.InternalProperties;
import com.ibm.streamsx.topology.internal.graph.GraphKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;
import com.ibm.streamsx.topology.internal.streams.InvokeSc;

/**
 * Create a Streams bundle using SPL compiler (sc) from a local Streams install.
 */
public class BundleStreamsContext extends ToolkitStreamsContext {

    static final Logger trace = Topology.TOPOLOGY_LOGGER;

    private final boolean standalone;
    private final boolean keepBundle;

    public BundleStreamsContext(boolean standalone, boolean keepBundle) {
        this.standalone = standalone;
        this.keepBundle = keepBundle;
    }

    @Override
    public Type getType() {
        return standalone ? Type.STANDALONE_BUNDLE : Type.BUNDLE;
    }
    
    @Override
    public Future<File> _submit(AppEntity entity) throws Exception {
        return super._submit(entity);
    }
    
    @Override
    protected Future<File> action(AppEntity entity) throws Exception {
        
        JsonObject submission = entity.submission;
        
        JsonObject deploy = deploy(submission);
        if (deploy.has(APP_DIR))
            deploy.add(TOOLKIT_DIR, deploy.get(APP_DIR));
    	
    	File appDir = super.action(entity).get();
    	Future<File> bundle = doSPLCompile(appDir, submission);
    	   
    	// Create a Job Config Overlays file if this is creating
    	// a sab for subsequent distributed deployment
    	// or keepArtifacts is set.
    	if (!standalone && (keepBundle || keepArtifacts(submission)))
    	    createJobConfigOverlayFile(submission, deploy, bundle.get().getParentFile());
    	
    	// If user asked for the SAB or asked to keep the SAB explicitly
    	if (keepBundle || keepArtifacts(submission)) {
    		final JsonObject submissionResult = GsonUtilities.objectCreate(submission, RemoteContext.SUBMISSION_RESULTS);
    		submissionResult.addProperty(SubmissionResultsKeys.BUNDLE_PATH, bundle.get().getAbsolutePath());
    	}
    	
    	return bundle;
    }
    

    
    private Future<File> doSPLCompile(File appDir, JsonObject submission) throws Exception {
    	 	
    	JsonObject deploy = deploy(submission);
    	JsonObject graph = GraphKeys.graph(submission);
    	
        String namespace = splAppNamespace(graph);
        String name = splAppName(graph);

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
