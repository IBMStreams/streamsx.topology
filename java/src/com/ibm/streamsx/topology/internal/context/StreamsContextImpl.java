/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.ContextProperties.SUBMISSION_PARAMS;
import static com.ibm.streamsx.topology.context.ContextProperties.TRACING_LEVEL;
import static com.ibm.streamsx.topology.context.ContextProperties.VMARGS;
import static com.ibm.streamsx.topology.context.JobProperties.CONFIG;
import static com.ibm.streamsx.topology.context.JobProperties.DATA_DIRECTORY;
import static com.ibm.streamsx.topology.context.JobProperties.GROUP;
import static com.ibm.streamsx.topology.context.JobProperties.NAME;
import static com.ibm.streamsx.topology.context.JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION;
import static com.ibm.streamsx.topology.context.JobProperties.PRELOAD_APPLICATION_BUNDLES;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.DEPLOY;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.JOB_CONFIG_OVERLAYS;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities.gson;
import static com.ibm.streamsx.topology.internal.streams.Util.getConfigEntry;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.JobProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.context.remote.DeployKeys;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities;
import com.ibm.streamsx.topology.internal.streams.JobConfigOverlay;
import com.ibm.streamsx.topology.jobconfig.JobConfig;

abstract class StreamsContextImpl<T> implements StreamsContext<T> {

    @Override
    public final Future<T> submit(Topology topology) throws Exception {
        return submit(topology, Collections.emptyMap());
    }
    
    /**
     * Force a copy of the config to avoid modifying the passed in config.
     */
    @Override
    public final Future<T> submit(Topology topology, Map<String, Object> config) throws Exception {
        return _submit(topology, new HashMap<>(config));
    }
    
    /**
     * The workhorse for submit, can modify its config so this
     * variant (_submit) should be called by sub-classes instead
     * of (submit) to allow data to be exchanged through config values.
     */
    abstract Future<T> _submit(Topology topology, Map<String, Object> config) throws Exception;

    /**
     * Default implementation.
     * @return true; the context supports the topology
     */
    @Override
    public boolean isSupported(Topology topology) {
        return true;
    }
    
    @Override
    public final Future<T> submit(JSONObject submission) throws Exception {
    	return _submit(JSON4JUtilities.gson(submission));
    }
    
    /**
     * Workhorse for handling a JSON graph. Sub-classes use Gson
     * to allow sharing code between remote (non-install) contexts
     * and product install contexts.
     */
    Future<T> _submit(JsonObject submission) throws Exception {
        throw new UnsupportedOperationException();
    } 
    
    /**
     * Create JSON form of the submission from a topology and config.
     * @throws Exception 
     */
    JsonObject createSubmission(Topology app, Map<String,Object> config) throws Exception {
        
        app.finalizeGraph(getType());
        
        JsonObject deploy = new JsonObject();        
        addConfigToDeploy(deploy, config);
        
        
        
        JsonObject submission = new JsonObject();
        submission.add(DEPLOY,deploy);
        submission.add(SUBMISSION_GRAPH, gson(app.builder().complete()));
        
        return submission;
    }
    
    @SuppressWarnings("unchecked")
    private static JsonElement convertConfigValue(Object value) {
        if (value instanceof Boolean)
            return new JsonPrimitive((Boolean) value); 
        else if (value instanceof Number)
            return new JsonPrimitive((Number) value);
        else if (value instanceof String) {
            return new JsonPrimitive((String) value);
        } else if (value instanceof JSONObject) {
            return gson((JSONObject) value);
        } else if (value instanceof Collection) {
            JsonArray array = new JsonArray();
            for (Object e : (Collection<Object>) value) {
                array.add(convertConfigValue(e));
            }
            return array;
        } else if (value instanceof File) {
            return new JsonPrimitive(((File) value).getAbsolutePath());
        }
        throw new IllegalArgumentException(value.getClass().getName());
    }
    
    /**
     * Config keys that are skipped from being added generically in
     * the deploy JSON.
     */
    private static final Set<String> CONFIG_SKIP_KEYS = new HashSet<>();
    static {
        // Keys handled by Job Config overlays
        
        // ContextProperties
        Collections.addAll(CONFIG_SKIP_KEYS, TRACING_LEVEL, SUBMISSION_PARAMS);
        
        // JobProperties
        Collections.addAll(CONFIG_SKIP_KEYS, CONFIG, NAME, GROUP, DATA_DIRECTORY,
                OVERRIDE_RESOURCE_LOAD_PROTECTION, PRELOAD_APPLICATION_BUNDLES);
    }
    
    /**
     * Convert the config information into the JSON deploy.
     */
    private void addConfigToDeploy(JsonObject deploy, Map<String,Object> config) {
        
        // For job configuration information we convert to a job
        // config overlay
        JobConfig jc = JobConfig.fromProperties(config);
        JobConfigOverlay jco = new JobConfigOverlay(jc);       
        jco.fullOverlayAsJSON(deploy);
        
        for (String key : config.keySet()) {
            if (CONFIG_SKIP_KEYS.contains(key))
                continue;
            try {
                deploy.add(key, convertConfigValue(config.get(key)));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown type for config:" + key + " - " + e.getMessage());
            }
        }
    }
}
