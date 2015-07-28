/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import java.lang.reflect.Type;

import com.ibm.streamsx.topology.TopologyElement;

public abstract class TupleContainer<T> extends TopologyItem {

    private final Type tupleType;
    
    protected TupleContainer(TopologyElement fe, Type tupleType) {
        super(fe);
        this.tupleType = tupleType;
    }

    @SuppressWarnings("unchecked")
    public final Class<T> getTupleClass() {
        if (tupleType instanceof Class)
            return (Class<T>) tupleType;
        return null;
    }
    
    public final Type getTupleType() {
        return tupleType;
    }
}
