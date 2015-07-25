/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.tuple.Keyable;

/**
 * Obtain a hash for a Keyable object's key.
 */
public final class KeyableHasher implements ToIntFunction<Keyable<?>> {
    private static final long serialVersionUID = 1L;
    
    public final static KeyableHasher SINGLETON = new KeyableHasher();
    
    private KeyableHasher() {}

    @Override
    public int applyAsInt(Keyable<?> tuple) {
        return tuple.getKey().hashCode();
    }
    
    // Enforce a singleton
    private Object readResolve() {
        return SINGLETON;
    }
}
