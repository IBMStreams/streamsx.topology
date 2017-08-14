/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import com.ibm.streamsx.topology.function.ToIntFunction;

/**
 * Obtain a hash for an Object
 */
public final class ObjectHasher implements ToIntFunction<Object> {
    private static final long serialVersionUID = 1L;
    
    public final static ObjectHasher SINGLETON = new ObjectHasher();
    
    private ObjectHasher() {}

    @Override
    public int applyAsInt(Object tuple) {
        return tuple.hashCode();
    }
    
    // Enforce a singleton
    private Object readResolve() {
        return SINGLETON;
    }
}
