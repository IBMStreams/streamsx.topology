/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.graph;

import static com.ibm.streamsx.topology.generator.spl.SPLGenerator.getSPLCompatibleName;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.generator.spl.SPLGenerator;

/**
 * Keys in the JSON graph object for job submission.
 * 
 * Keys whose field name starts with CFG are stored in
 * the "config" area of the graph.
 */
public interface GraphKeys {
    
    /**
     * Key for graph in top-level submission object.
     */
    String GRAPH = "graph";
    
    /**
     * Key for graph config in graph object.
     */
    String CONFIG = "config";
    
    /**
     * Get graph object from submission.
     */
    static JsonObject graph(JsonObject submission) {
        return object(submission, GRAPH);
    }
    
    /**
     * Get graph config object from submission.
     */
    static JsonObject graphConfig(JsonObject submission) {
        return objectCreate(submission, GRAPH, CONFIG);
    }
    
    /**
     * user supplied application name.
     */
    String NAME = "name";
    
    /**
     * user supplied application namespace.
     */
    String NAMESPACE = "namespace";
    
    /**
     * SPL application namespace.
     */
    String SPL_NAMESPACE = "splNamespace";
    
    /**
     * SPL application name.
     */
    String SPL_NAME = "splName";
    
    /**
     * User supplied application name.
     */
    static String appName(JsonObject graph) {
        return jstring(graph, NAME);
    }
    /**
     * User supplied application namespace.
     */
    static String appNamespace(JsonObject graph) {
        return jstring(graph, NAMESPACE);
    }
    
    /**
     * SPL application name derived from appName().
     */
    static String splAppName(JsonObject graph) {
        String name = jstring(graph, SPL_NAME);
        if (name == null) {
            name = appName(graph);
            name = getSPLCompatibleName(name);
            graph.addProperty(SPL_NAME, name);
        }
        return name;
    }
    
    /**
     * SPL application name derived from appName().
     */
    static String splAppNamespace(JsonObject graph) {
        String ns = jstring(graph, SPL_NAMESPACE);
        if (ns == null) {
            ns = appNamespace(graph);
            if (ns != null) {
                StringBuilder nsb = new StringBuilder();
                StringTokenizer st = new StringTokenizer(ns, ".");
                while (st.hasMoreTokens()) {
                    if (nsb.length() != 0)
                        nsb.append(".");
                    nsb.append(getSPLCompatibleName(st.nextToken()));
                }
                ns = nsb.toString();
                // Account for the full app name being > 255 (with a margin)
                // when including file suffixes and separators.
                if (ns.length() + splAppName(graph).length() > 240) {
                    ns = "__spl_ns._" + 
                        SPLGenerator.md5Name(appNamespace(graph).getBytes(StandardCharsets.UTF_8));
                }
                graph.addProperty(SPL_NAMESPACE, ns);
            }
        }
        return ns;
    }
    
    /**
     * Does the graph include isolate virtual markers.
     * Boolean.
     */
    String CFG_HAS_ISOLATE = "hasIsolate";
    
    /**
     * Mapping of colocation keys to actual colocate tag.
     * Object containing string to string mapping. 
     */
    String CFG_COLOCATE_TAG_MAPPING = "colocateTagMapping";
    
    /**
     * Object of colocate tags to count of composites
     * that use them. Each id maps to a JSON object
     * containing a count.
     */
    String CFG_COLOCATE_IDS = "colocateIds";

    /**
     * Version of Streams the graph is targeted at.
     * (runtime or compile if compile version not set). 
     */
    String CFG_STREAMS_VERSION = "streamsVersion";
    
    /**
     * Version of Streams the graph is targeted at for compile.
     * Optional. 
     */
    String CFG_STREAMS_COMPILE_VERSION = "streamsCompileVersion";
}

