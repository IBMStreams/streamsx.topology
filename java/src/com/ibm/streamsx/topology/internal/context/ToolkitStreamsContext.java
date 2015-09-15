/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

public class ToolkitStreamsContext extends StreamsContextImpl<File> {

    Map<String, Object> graphItems;

    @Override
    public Type getType() {
        return Type.TOOLKIT;
    }

    @Override
    public Future<File> submit(Topology app, Map<String, Object> config)
            throws Exception {

        if (config == null)
            config = new HashMap<>();

        // If the toolkit path is not given, then create one in the
        // currrent directory.
        if (!config.containsKey(ContextProperties.TOOLKIT_DIR)) {
            config.put(ContextProperties.TOOLKIT_DIR, Files
                    .createTempDirectory(Paths.get(""), "tk").toAbsolutePath().toString());
        }

        File toolkitRoot = new File((String) config.get(ContextProperties.TOOLKIT_DIR));

        makeDirectoryStructure(toolkitRoot,
                (String) app.builder().json().get("namespace"));

        graphItems = app.finalizeGraph(getType(), config);
        
        addConfigToJSON(app.builder().getConfig(), config);

        generateSPL(app, config);
        return new CompletedFuture<File>(toolkitRoot);
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
    
    public static void main(String[] args) {
        JSONArray ja = new JSONArray();
        ja.add(new Integer(22));
    }

    private void generateSPL(Topology app, Map<String, Object> config)
            throws IOException {

        JSONObject json = app.builder().complete();

        // Create the SPL file, and save a copy of the JSON file.
        SPLGenerator generator = new  SPLGenerator();
        createNamespaceFile(json, config, "spl", generator.generateSPL(json));
        createNamespaceFile(json, config, "json", json.serialize());
    }

    private void createNamespaceFile(JSONObject json,
            Map<String, Object> config, String suffix, String content)
            throws IOException {

        String namespace = (String) json.get("namespace");
        String name = (String) json.get("name");

        File f = new File((String) config.get(ContextProperties.TOOLKIT_DIR),
                namespace + "/" + name + "." + suffix);
        PrintWriter splFile = new PrintWriter(f, "UTF-8");
        // splFile.print(app.splgraph().toSPLString());
        splFile.print(content);
        splFile.flush();
        splFile.close();
    }

    private void makeDirectoryStructure(File toolkitRoot, String namespace)
            throws Exception {

        File tkNamespace = new File(toolkitRoot, namespace);
        File tkImplLib = new File(toolkitRoot, "impl/lib");
        File tkEtc = new File(toolkitRoot, "etc");
        File tkOpt = new File(toolkitRoot, "opt");

        tkImplLib.mkdirs();
        tkNamespace.mkdirs();
        tkEtc.mkdir();
        tkOpt.mkdir();
    }

}
