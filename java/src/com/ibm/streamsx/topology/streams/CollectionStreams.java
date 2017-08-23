/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.streams;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
    public static class FlattenCollection<C extends Collection<T>, T> implements Function<C, Iterable<T>> {
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
        return stream.flatMap(new FlattenCollection<C, T>());
    }
    
    
    public static <M extends Map<K,V>, K, V> TStream<SimpleImmutableEntry<K,V>> flattenMap(TStream<M> stream) {
        return stream.flatMap(new FlattenMap<M, K, V>());
    }
    
    public static class FlattenMap <M extends Map<K,V>, K, V> implements Function<M, Iterable<SimpleImmutableEntry<K,V>>> {
        private static final long serialVersionUID = 1L;
        
        @Override
        public Iterable<SimpleImmutableEntry<K, V>> apply(M v) {
            List<SimpleImmutableEntry<K,V>> mapEntries = new ArrayList<>(v.size());
            for (K key : v.keySet()) {
                mapEntries.add(new SimpleImmutableEntry<>(key, v.get(key)));
            }
            return mapEntries;
        }
        
    }
}
