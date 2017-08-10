/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.gson;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.gson.JsonObject;

/**
 * Accesses JSON4J indirectly to avoid a dependency on it.
 * This allows client side code where Streams is not installed.
 *
 */
public class JSON4JBridge {
    
    public static boolean isJson4J(Object o) {
        return "com.ibm.json.java.JSONObject".equals(o.getClass().getName());
    }
     
    public static JsonObject fromJSON4J(Object o) {
        assert isJson4J(o);
        
        try {
            Class<?> j4ju = Class.forName("com.ibm.streamsx.topology.internal.json4j.JSON4JUtilities");
            
            Method gson = j4ju.getMethod("gson", o.getClass());
            
            return (JsonObject) gson.invoke(null, o);
        } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

}