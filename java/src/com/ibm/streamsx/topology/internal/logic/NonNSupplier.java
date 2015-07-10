/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;

public class NonNSupplier<T> implements Function<Long, T>, WrapperFunction {

    private static final long serialVersionUID = 1L;

    private final Supplier<T> data;

    public NonNSupplier(Supplier<T> data) {
        this.data = data;
    }

    @Override
    public T apply(Long ignored) {
        return data.get();
    }

    @Override
    public Object getWrappedFunction() {
        return data;
    }
}
