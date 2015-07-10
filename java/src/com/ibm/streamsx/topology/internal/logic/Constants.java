/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.List;

import com.ibm.streamsx.topology.function.Supplier;

public final class Constants<T> implements Supplier<Iterable<T>> {
    private static final long serialVersionUID = 1L;
    
    private final List<T> data;
    
    public Constants(List<T> data) {
        this.data = data;
    }
    @Override
    public Iterable<T> get() {
        return data;
    }
}