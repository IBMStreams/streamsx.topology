/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.Iterator;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;

public class EndlessSupplier<T> implements Supplier<Iterable<T>>,
        WrapperFunction {
    /**
             * 
             */
    private static final long serialVersionUID = 1L;

    public static <T> Supplier<Iterable<T>> supplier(final Supplier<T> data) {
        return new EndlessSupplier<T>(new NonNSupplier<T>(data));
    }

    public static <T> Supplier<Iterable<T>> supplierN(
            final Function<Long, T> data) {
        return new EndlessSupplier<T>(data);
    }

    private final Function<Long, T> supplier;
    private long seq;

    public EndlessSupplier(Function<Long, T> supplier) {
        this.supplier = supplier;
    }

    @Override
    public Object getWrappedFunction() {
        return supplier;
    }

    @Override
    public final Iterable<T> get() {
        return new Iterable<T>() {

            @Override
            public Iterator<T> iterator() {
                return new Iterator<T>() {

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public T next() {
                        return supplier.apply(seq++);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();

                    }
                };
            }

        };
    }
}
