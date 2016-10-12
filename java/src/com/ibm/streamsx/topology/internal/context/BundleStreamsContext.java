/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.internal.context.remote.ToolkitRemoteContext;
import com.ibm.streamsx.topology.internal.core.InternalProperties;
import com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities;
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
    public Future<File> submit(Topology app, Map<String, Object> config)
            throws Exception {

        if (config == null)
            config = new HashMap<>();

        File appDir;
        if (!config.containsKey(ContextProperties.APP_DIR)) {
            appDir = Files.createTempDirectory("apptk").toFile();
            config.put(ContextProperties.APP_DIR, appDir.getAbsolutePath());

        } else {
            appDir = new File((String) (config.get(ContextProperties.APP_DIR)));
        }
        config.put(ContextProperties.TOOLKIT_DIR, appDir.getAbsolutePath());

        File appDirA = super.submit(app, config).get();
        
        JSONObject jsonGraph = app.builder().complete();
        
        JSONObject submission = new JSONObject();
        JSONObject submissionDeploy = new JSONObject();
        submission.put(SUBMISSION_DEPLOY, submissionDeploy);
        if (config.containsKey(KEEP_ARTIFACTS)) {
            submissionDeploy.put(KEEP_ARTIFACTS, config.get(KEEP_ARTIFACTS));
        }
        submission.put(SUBMISSION_GRAPH, jsonGraph);
        
        return doSPLCompile(appDirA, submission);
    }
    
    @Override
    public Future<File> submit(JSONObject submission) throws Exception {
    	
    	File appDir = super.submit(submission).get();
    	return doSPLCompile(appDir,submission);
    }
    
    private Future<File> doSPLCompile(File appDir, JSONObject submission) throws Exception {
    	 	
    	JSONObject deployInfo = (JSONObject)  submission.get(SUBMISSION_DEPLOY);
    	JSONObject jsonGraph = (JSONObject) submission.get(SUBMISSION_GRAPH);
    	
        String namespace = (String) jsonGraph.get("namespace");
        String name = (String) jsonGraph.get("name");

        InvokeSc sc = new InvokeSc(deployInfo, standalone, namespace, name, appDir);
        
        // Add the toolkits
        JSONObject graphConfig  = (JSONObject) jsonGraph.get("config");
        if (graphConfig != null) {
            JSONObject splConfig = (JSONObject) graphConfig.get("spl");
            if (splConfig != null) {
                JSONArray toolkits = (JSONArray) splConfig.get(InternalProperties.TOOLKITS_JSON);
                if (toolkits != null) {
                    for (Object obj : toolkits) {
                        JSONObject tkinfo = (JSONObject) obj;
                        String root = (String) tkinfo.get("root");
                        if (root != null)
                            sc.addToolkit(new File(root));
                    }
                }
            }
        }

        sc.invoke();

        File outputDir = new File(appDir, "output");
        String bundleName = namespace + "." + name + ".sab";
        File bundle = new File(outputDir, bundleName);

        File localBundle = new File(bundle.getName());

        Files.copy(bundle.toPath(), localBundle.toPath(),
                StandardCopyOption.REPLACE_EXISTING);

        if (!ToolkitRemoteContext.deleteToolkit(appDir, JSON4JUtilities.gson(deployInfo)))
            trace.info("Keeping toolkit at: " + appDir.getAbsolutePath());

        trace.info("Streams Application Bundle produced: "
                + localBundle.getName());

        return new CompletedFuture<File>(localBundle);
    }

    
}
