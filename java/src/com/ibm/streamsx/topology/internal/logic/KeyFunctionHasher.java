/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.ToIntFunction;

/**
 * Obtain a hash for a key extracted from a tuple.
 */
public final class KeyFunctionHasher<T,K> implements ToIntFunction<T>, WrapperFunction {
    private static final long serialVersionUID = 1L;
    
    private final Function<T,K> keyFunction;
    public KeyFunctionHasher(Function<T,K> keyFunction) {
        this.keyFunction = keyFunction;
    }
    
    @Override
    public int applyAsInt(T tuple) {
        return keyFunction.apply(tuple).hashCode();
    }

    @Override
    public Object getWrappedFunction() {
        return keyFunction;
    }
}
