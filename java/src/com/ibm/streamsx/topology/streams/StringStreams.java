/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.streams;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;

/**
 * Utilities for streams containing {@code String} tuples.
 * 
 */
public class StringStreams {
    /**
     * Create a filtered stream that contains {@code String} tuples containing a
     * specific term. An input tuple {@code t} is present on the filtered stream
     * if {@code t.contains(term)} returns true.
     * 
     * @param stream
     *            Input stream
     * @param term
     *            Term to
     * @return Filtered stream of tuples that contain {@code term}.
     */
    public static TStream<String> contains(TStream<String> stream,
            final String term) {

        return stream.filter(new Predicate<String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean test(String v1) {
                return v1.contains(term);
            }
        });
    }

    /**
     * Create a filtered stream that contains {@code String} tuples starting
     * with a specific term. An input tuple {@code t} is present on the filtered
     * stream if {@code t.startsWith(term)} returns true.
     * 
     * @param stream
     *            Input stream
     * @param term
     *            Term to
     * @return Filtered stream of tuples that contain {@code term}.
     */
    public static TStream<String> startsWith(TStream<String> stream,
            final String term) {

        return stream.filter(new Predicate<String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public boolean test(String v1) {
                return v1.startsWith(term);
            }
        });
    }

    /**
     * Create a stream that converts each input tuple to its {@code String}
     * representation using {@code toString()}.
     * 
     * @param stream
     *            Stream to be converted to {@code String} values
     * @return Stream containing {@code String} representations of each tuple on
     *         {@code stream}.
     */
    public static <T> TStream<String> toString(TStream<T> stream) {
        TStream<String> toString = stream.transform(new Function<T, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public String apply(T tuple) {
                return tuple.toString();
            }
        });
        
        return toString;
    }
}
