/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.core;

public interface InternalProperties {
    
    String PREFIX = "topology.internal.";
    
    /**
     * Properties with the prefix SPL_PREFIX ({@value}
     * are turned into JSON properties in an {@code spl}
     * JSON config object the property name stripped of
     * the prefix.
     * 
     * E.g.
     * topology.internal.spl.toolkits becomes toolkits
     * in the spl JSON object.
     */
    String SPL_PREFIX = PREFIX + "spl.";
    
    /**
     * List of toolkits & toolkit dependencies used by the topology.
     * JSONArray containing a JSON object with fields:
     * name - Name of toolkit
     * version - dependency version string
     * root (optional) - location of toolkit.
     * 
     */
    String TOOLKITS_JSON = "toolkits";
    String TOOLKITS = SPL_PREFIX + TOOLKITS_JSON;
}
