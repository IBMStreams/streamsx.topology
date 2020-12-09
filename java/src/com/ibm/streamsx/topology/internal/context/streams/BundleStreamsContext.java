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
import com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext;
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

    @SuppressWarnings("deprecation")
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
        
        JsonObject graph = object(submission, RemoteContext.SUBMISSION_GRAPH);
        String mainCompositeKind = jstring(graph, "mainComposite");
        
        JsonObject deploy = deploy(submission);
        
        File appDir;
        if (mainCompositeKind != null) {
            // Single main composite, no need to generate a toolkit before the SPL compile step.
            String namespace;
            String name;
            int sep = mainCompositeKind.indexOf("::");
            if (sep != -1) {
                namespace = mainCompositeKind.substring(0, sep);
                name = mainCompositeKind.substring(sep+2);
            } else {
                // main composite w/o namespace.
                // Add an empty "splNamespace" for the compile step to the graph.
                // If we did not do this, or added 'null', the "namespace" from the graph
                // would be used for the compiler. This is the namespace of the _topology_, to which the
                // main composite has been added. The namespace of the topology is always the SPL-namespace
                // of the main composite extended by '._spl' or '_spl'. So, we tried to compile '_spl::Main'
                // instead of 'Main' --> compiler error '_spl::Main not found'.
                namespace = "";
                name = mainCompositeKind;
            }
            graph.addProperty(GraphKeys.SPL_NAMESPACE, namespace);
            graph.addProperty(GraphKeys.SPL_NAME, name);
            
            appDir = Files.createTempDirectory("tk").toFile();
            ToolkitRemoteContext.setupJobConfigOverlays(deploy, graph);
        } else {
            // generate a an SPL toolkit
            if (deploy.has(APP_DIR))
                deploy.add(TOOLKIT_DIR, deploy.get(APP_DIR));
            
            appDir = super.action(entity).get();
        }
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

        final boolean haveNamespace = namespace != null && !namespace.isEmpty();
        File outputDir = new File(appDir, "output");
        String bundleName = haveNamespace? namespace + "." + name + ".sab": name + ".sab";
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
