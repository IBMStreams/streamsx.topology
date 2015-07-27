/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.streams;

import java.util.Collection;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.Function;

/**
 * Utilities for streams tuples that are collections of values.
 */
public class CollectionStreams {
    
    /**
     * Function that flattens a collection when passed to
     * {@link TStream#multiTransform(Function)}.
     *
     * @param <C> Type of the collection.
     * @param <T> Type of the elements.
     */
    public static final class Flatten<C extends Collection<T>, T> implements Function<C, Iterable<T>> {
        private static final long serialVersionUID = 1L;

        /**
         * Just returns the collection, {@link TStream#multiTransform(Function)} will actually
         * perform the flattening.
         */
        @Override
        public Iterable<T> apply(C v) {
            return v;
        }
    }

    /**'
     * Flatten a stream containing collections into a stream containing
     * the individual elements. Elements are added to the returned
     * stream in the order of the iterator for the input tuple.
     * 
     * @param stream Stream to be flattened.
     * @return Flattened stream.
     */
    public static <C extends Collection<T>, T> TStream<T> flatten(TStream<C> stream) {
        return stream.multiTransform(new Flatten<C, T>());
    }
}
