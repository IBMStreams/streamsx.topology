/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.spi.builder.SourceInfo.SOURCE_LOCATIONS;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;


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
        
        com.ibm.streamsx.topology.spi.builder.SourceInfo.addSourceInfo(bop._json(), calledClass);    
    }
    
    private final static String[] INVOKE_INFO_KEYS = {SOURCE_LOCATIONS, "layout"};
    
    public static void setInvocationInfo(BOperatorInvocation bop, JsonObject invokeInfo) {
        
        // Check for a mangled operator name
        // Currently names is under the control of the topology implementation
        // not its callers.
        if (bop.layout().has("names")) {
            JsonObject layout = GsonUtilities.objectCreate(invokeInfo, "layout");
            layout.add("names", bop.layout().get("names"));
        }
        
        for (String key : INVOKE_INFO_KEYS) {
            if (invokeInfo.has(key))
                bop._json().add(key, invokeInfo.get(key));
        }
    }
}
