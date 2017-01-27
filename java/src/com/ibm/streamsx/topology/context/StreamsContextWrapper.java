/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
 */
package com.ibm.streamsx.topology.context;

import static com.ibm.streamsx.topology.context.ContextProperties.COMPILE_INSTALL_DIR;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.streams.Util;

class StreamsContextWrapper<T> implements StreamsContext<T> {
    
    static <C> StreamsContext<C> wrap(StreamsContext<C> context) {
       return new StreamsContextWrapper<>(context);
    }
    
    private final StreamsContext<T> context;
    private StreamsContextWrapper(StreamsContext<T> context) {
        this.context = context;
    }
    
    public StreamsContext.Type getType() {
        return context.getType();
    }
    public boolean isSupported(Topology topology) {
        return context.isSupported(topology);
    }
    public Future<T> submit(Topology topology) throws Exception {
        return context.submit(topology);
    }
    public Future<T> submit(Topology topology, Map<String, Object> config) throws Exception {
        setCompileVersion(topology, config);
        // Copy the context to leave the caller's untouched
        return context.submit(topology, new HashMap<>(config));
    }
    public Future<T> submit(JSONObject submission) throws Exception {
        // Copy the context to leave the caller's untouched
        return context.submit((JSONObject) submission.clone());
    }

    /**
     * Set the version of the compiler to be used if
     * a specific install has been set using: COMPILE_INSTALL_DIR.
     * 
     * Sets the config property streamsCompileVersion in the graph (JSON)
     * if COMPILE_INSTALL_DIR is set. If not set then the config property
     * streamsVersion is used, which is the version running the build of the
     * topology..
     */
    private void setCompileVersion(Topology topology, Map<String, Object> config) throws IOException {
        
        if (config.containsKey(COMPILE_INSTALL_DIR)) {
            JSONObject cfg = new JSONObject();
            cfg.put(COMPILE_INSTALL_DIR,
                    Util.getConfigEntry(config, COMPILE_INSTALL_DIR, String.class));
            
            String compileInstall = Util.getStreamsInstall(cfg, COMPILE_INSTALL_DIR);
            
            File pv = new File(compileInstall, ".product");
            try (FileInputStream pvis = new FileInputStream(pv)) {
                Properties pvp = new Properties();
                pvp.load(pvis);
                if (pvp.containsKey("Version")) {
                    topology.builder().getConfig().put("streamsCompileVersion", pvp.get("Version"));
                }                  
            }          
        }
    }
}
