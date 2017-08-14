/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.core;

import static com.ibm.streamsx.topology.spi.builder.SourceInfo.SOURCE_LOCATIONS;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;


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
    
    public static void setSourceInfo(BOperatorInvocation bop, JsonObject config) {
        
        if (!config.has(SOURCE_LOCATIONS))
            return;
        
        bop._json().add(SOURCE_LOCATIONS, config.get(SOURCE_LOCATIONS));
    }
}
