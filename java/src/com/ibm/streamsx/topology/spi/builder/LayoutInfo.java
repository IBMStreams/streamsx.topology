/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.spi.builder;

import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectCreate;

import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonObject;

/**
 * Layout hints for the Streams console.
 */
public class LayoutInfo {
    
    private static JsonObject layout(JsonObject invokeInfo) {
        return objectCreate(invokeInfo, "layout");
    }
    
    /**
     * Provide a hint that the operator invocation should be hidden.
     * @param invokeInfo Operator invocation information.
     */
    public static void hide(JsonObject invokeInfo) {
        layout(invokeInfo).addProperty("hidden", true);
    }
    
    /**
     * Provide a hint that the operator invocation kind should
     * be set to {@code kind} instead of its SPL kind.
     * @param invokeInfo Operator invocation information.
     * @param kind Kind to display for operator invocation.
     */
    public static void kind(JsonObject invokeInfo, String kind) {
        layout(invokeInfo).addProperty("kind", kind);
    }
    
    private static AtomicLong nextId = new AtomicLong();
    
    /**
     * Provide a hint that that an operator invocation should be
     * grouped with other operator invocations.
     * 
     * All operator invocations in a topology within the same {@code id}
     * will be shown as a single operator invocation in the Streams console.
     * The name and kind of the invocation are taken from {@code name} and {@code kind}.
     * 
     * @param invokeInfo Operator invocation information.

     * @param kind Kind to display for the group operator invocation.
     * @param name Name of the group operator invocation.
     * @param id Group identifier, if {@code null} a new identifier is created.
     * 
     * @return {@code id} if it was non-null, otherwise a new identifier.
     */
    public static String group(JsonObject invokeInfo, String kind, String name, String id) {
        
        if (id == null)
            id = "__spl_lg_java_" + nextId.getAndIncrement();
        
        JsonObject group = new JsonObject();
        
        group.addProperty("kind", kind);
        group.addProperty("name", name);
        group.addProperty("id", id);
        
        layout(invokeInfo).add("group", group);
        
        return id;
    }
    
    /**
     * Group multiple operators together.
     * @param invokeInfo Operator invocation with existing group information created by
     * {@link #group(JsonObject, String, String, String)}.
     * @param others Operator invocations to be grouped with operator invocation
     * represented by {@code invokeInfo}.
     * @return Group identifier.
     */
    public static String group(JsonObject invokeInfo, JsonObject ...others) {
        assert layout(invokeInfo).has("group");
        JsonObject group = layout(invokeInfo).get("group").getAsJsonObject();
        for (JsonObject other : others) {
            layout(other).add("group", group);
        }
        return jstring(group, "id");
    }


}
