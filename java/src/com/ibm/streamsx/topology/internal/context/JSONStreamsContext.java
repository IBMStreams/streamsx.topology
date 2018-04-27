/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import static com.ibm.streamsx.topology.context.ContextProperties.SUBMISSION_PARAMS;
import static com.ibm.streamsx.topology.context.ContextProperties.TRACING_LEVEL;
import static com.ibm.streamsx.topology.context.JobProperties.CONFIG;
import static com.ibm.streamsx.topology.context.JobProperties.DATA_DIRECTORY;
import static com.ibm.streamsx.topology.context.JobProperties.GROUP;
import static com.ibm.streamsx.topology.context.JobProperties.NAME;
import static com.ibm.streamsx.topology.context.JobProperties.OVERRIDE_RESOURCE_LOAD_PROTECTION;
import static com.ibm.streamsx.topology.context.JobProperties.PRELOAD_APPLICATION_BUNDLES;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.DEPLOY;

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
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.context.remote.RemoteContexts;
import com.ibm.streamsx.topology.internal.gson.JSON4JBridge;
import com.ibm.streamsx.topology.internal.streams.JobConfigOverlay;
import com.ibm.streamsx.topology.internal.messages.Messages;
import com.ibm.streamsx.topology.jobconfig.JobConfig;

public abstract class JSONStreamsContext<T> extends StreamsContextImpl<T> {
    
    public static class AppEntity {
        public final Topology app;
        public final Map<String,Object> config;
        public JsonObject submission;
        public final Map<Object,Object> saved = Collections.synchronizedMap(new HashMap<>());
        
        AppEntity(Topology app, Map<String,Object> config) throws Exception {
            this.app = app;
            this.config = config;
            
        }
        AppEntity(JsonObject submission) {
            this.app = null;
            this.config = null;
            this.submission = submission;
        }
        
        public Object getSavedObject(Object key) {
            return saved.get(key);
        }
        
        public <T> T saveObject(Object key, T value) {
            saved.put(key, value);
            return value;
        }
    }

    /**
     * Force a copy of the config to avoid modifying the passed in config.
     */
    @Override
    public final Future<T> submit(Topology app, Map<String, Object> config) throws Exception {
        return _submit(new AppEntity(app, new HashMap<>(config)));
    }
    
    protected Future<T> _submit(AppEntity entity) throws Exception {
        preSubmit(entity);
        if (entity.submission == null)
            createSubmission(entity);
        return postSubmit(entity, action(entity));
    }
    
    /**
     * Pre-submit hook when submitting a Topology.
     */
    protected void preSubmit(AppEntity entity) {        
    }
    
    /**
     * Post-submit hook when submitting a Topology.
     */
    protected Future<T> postSubmit(AppEntity entity, Future<T> future) throws Exception{
        RemoteContexts.writeResultsToFile(entity.submission);
        return future;
    }
    
    @Override
    public final Future<T> submit(JsonObject submission) throws Exception {
    	return _submit(new AppEntity(submission));
    }
    
    /**
     * Workhorse for handling a JSON graph. Sub-classes use Gson
     * to allow sharing code between remote (non-install) contexts
     * and product install contexts.
     */
    protected abstract Future<T> action(AppEntity entity) throws Exception;
    
    /**
     * Create JSON form of the submission from a topology and config.
     * @throws Exception 
     */
    private void createSubmission(AppEntity entity) throws Exception {
        assert entity.submission == null;
        
        JsonObject submission = new JsonObject();
        
        entity.app.finalizeGraph(getType());
        
        JsonObject deploy = new JsonObject();        
        addConfigToDeploy(deploy, entity.config);
        deploy.addProperty("contextType", this.getType().name());
        
        submission.add(DEPLOY,deploy);
        submission.add(SUBMISSION_GRAPH, entity.app.builder()._complete());
        
        entity.submission = submission;
    }
    
    @SuppressWarnings("unchecked")
    private static JsonElement convertConfigValue(Object value) {

        if (value instanceof Boolean)
            return new JsonPrimitive((Boolean) value); 
        else if (value instanceof Number)
            return new JsonPrimitive((Number) value);
        else if (value instanceof String) {
            return new JsonPrimitive((String) value);
        } else if (value instanceof JsonElement) {
            return (JsonElement) value;
        } else if (JSON4JBridge.isJson4J(value)) {
            return JSON4JBridge.fromJSON4J(value);
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
    private static void addConfigToDeploy(JsonObject deploy, Map<String,Object> config) {
        
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
                throw new IllegalArgumentException(Messages.getString("CONTEXT_UNKNOWN_TYPE_FOR_CONFIG", key, e.getMessage()));
            }
        }
    }
}
