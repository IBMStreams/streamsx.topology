/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities.gson;
import static com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities.json4j;
import static com.ibm.streamsx.topology.spi.SourceInfo.SOURCE_LOCATIONS;

import java.io.IOException;

import com.google.gson.JsonObject;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities;


public class SourceInfo {

    
    public static StackTraceElement getCaller(Class<?> calledClass) {

        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        
        boolean foundCalled = false;
        for (int i = 0; i < stack.length; i++) {
            StackTraceElement ste = stack[i];
            if (calledClass.getName().equals(ste.getClassName())) {
                foundCalled = true;
                continue;
            }
            
            if (foundCalled) {
                return ste;             
            }
        }
        
        return null;
    }
    
    
    public static void setSourceInfo(BOperatorInvocation bop, Class<?> calledClass) {
                
        JsonObject holder = new JsonObject();

        if (bop.json().containsKey(SOURCE_LOCATIONS)) {
            holder.add(SOURCE_LOCATIONS,
                    gson((JSONObject) (bop.json().get(SOURCE_LOCATIONS))));
        }
        
        com.ibm.streamsx.topology.spi.SourceInfo.addSourceInfo(holder, calledClass);
        
        setSourceInfo(bop, holder);       
    }
    
    public static void setSourceInfo(BOperatorInvocation bop, JsonObject config) {
        
        if (!config.has(SOURCE_LOCATIONS))
            return;
        
        JSONObject holder4j;
        try {
            holder4j = json4j(config);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        bop.json().put(SOURCE_LOCATIONS, holder4j.get(SOURCE_LOCATIONS));
    }
}
