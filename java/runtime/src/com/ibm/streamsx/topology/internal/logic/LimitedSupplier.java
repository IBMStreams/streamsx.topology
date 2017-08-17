/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.Iterator;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;

public class LimitedSupplier<T> implements Supplier<Iterable<T>>,
        WrapperFunction {
    /**
             * 
             */
    private static final long serialVersionUID = 1L;

    public static <T> Supplier<Iterable<T>> supplier(final Supplier<T> data,
            long count) {
        return new LimitedSupplier<T>(new NonNSupplier<T>(data), count);
    }

    public static <T> Supplier<Iterable<T>> supplierN(
            Function<Long, T> supplier, long count) {
        return new LimitedSupplier<T>(supplier, count);
    }

    private final Function<Long, T> supplier;
    private final long count;
    private long c;

    public LimitedSupplier(Function<Long, T> supplier, long count) {
        this.supplier = supplier;
        this.count = count;
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
                        return c < count;
                    }

                    @Override
                    public T next() {
                        return supplier.apply(c++);
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
