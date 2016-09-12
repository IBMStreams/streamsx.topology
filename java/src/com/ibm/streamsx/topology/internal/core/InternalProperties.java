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
    
    String TK_DIRS_JSON = "toolkits";
    String TK_DIRS = SPL_PREFIX + TK_DIRS_JSON;

}
