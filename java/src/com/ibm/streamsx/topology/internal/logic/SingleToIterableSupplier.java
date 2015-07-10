/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.Collections;

import com.ibm.streamsx.topology.function.Supplier;

public class SingleToIterableSupplier<T> implements Supplier<Iterable<T>>,
        WrapperFunction {
    /**
             * 
             */
    private static final long serialVersionUID = 1L;

    private final Supplier<T> data;

    public SingleToIterableSupplier(Supplier<T> data) {
        this.data = data;
    }

    @Override
    public Object getWrappedFunction() {
        return data;
    }

    @Override
    public final Iterable<T> get() {
        return Collections.singleton(data.get());
    }
}
