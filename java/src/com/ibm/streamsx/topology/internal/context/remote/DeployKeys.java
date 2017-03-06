/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;


import static com.ibm.streamsx.topology.context.ContextProperties.KEEP_ARTIFACTS;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.graph;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppName;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppNamespace;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.gson;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Date;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.internal.graph.GraphKeys;

/**
 * Keys in the JSON deploy object for job submission.
 */
public interface DeployKeys {
    
    /**
     * Key for deploy information in top-level submission object.
     */
    String DEPLOY = "deploy";
    
    /**
     * Get deploy object from submission.
     */
    static JsonObject deploy(JsonObject submission) {
        return object(submission, DEPLOY);
    }
    static boolean keepArtifacts(JsonObject submission) {;
        return jboolean(deploy(submission), KEEP_ARTIFACTS);
    }
    
    /**
     * Python information.
     * A JSON object with:
     * "prefix": sys.exec_prefix
     * "version": sys.version
     */
    String PYTHON = "python";
    
    /**
     * Streams 4.2 job config overlays. Expect value
     * is an array of job config overlays, though
     * only a single one is supported.
     */
    String JOB_CONFIG_OVERLAYS = "jobConfigOverlays";
    
    /**
     * Deployment config within a Job Config Overlay
     * or within the graph config element. The
     * graph config element is set during SPL
     * generation according to the requirements
     * of the graph.
     * (within JOB_CONFIG_OVERLAYS)
     */
    String DEPLOYMENT_CONFIG = "deploymentConfig";
    
    /**
     * Job configuration values.
     * (within JOB_CONFIG_OVERLAYS)
     */
    String JOB_CONFIG = "jobConfig";
    
    /**
     * Operation config values.
     * (within JOB_CONFIG_OVERLAYS)
     */
    String OPERATION_CONFIG = "operationConfig";
    
    /**
     * resource overload protection
     * Note: special user authority is required for use of this option.
     * (within OPERATION_CONFIG)
     */
    String OVERRIDE_RESOURCE_LOAD_PROTECTION = "overrideResourceLoadProtection";
    
    /**
     * Create a new JsonObject that contains the JOB_CONFIG_OVERLAYS
     * from  deploy
     */
    static JsonObject copyJobConfigOverlays(JsonObject deploy) {
        JsonObject jcos = new JsonObject();

        if (deploy.has(JOB_CONFIG_OVERLAYS))
            jcos.add(JOB_CONFIG_OVERLAYS, deploy.get(JOB_CONFIG_OVERLAYS));
        
        return jcos;
    }
    
    /**
     * Save a JobConfig overlay file.
     */
    static File createJobConfigOverlayFile(JsonObject submission, JsonObject deploy, File dir) throws IOException {

        if (deploy.has(DeployKeys.JOB_CONFIG_OVERLAYS)) {
            JsonObject graph = graph(submission);
            JsonObject graphConfig = object(graph, "config");

            boolean jcos_ok = jboolean(graphConfig, "supportsJobConfigOverlays");
            if (!jcos_ok)
                return null;

            String namespace = splAppNamespace(graph);
            String name = splAppName(graph);

            File jcf = new File(dir, namespace + "." + name + "_JobConfig.json");

            JsonObject jcos = copyJobConfigOverlays(deploy);
            jcos.addProperty("comment",
                    String.format("Job Config Overlays for %s::%s - generated %s", namespace, name, new Date()));

            String jcos_str = gson().toJson(jcos);

            Files.write(jcf.toPath(), jcos_str.getBytes(StandardCharsets.UTF_8));
            
            return jcf;
        }
        return null;
    }
}
