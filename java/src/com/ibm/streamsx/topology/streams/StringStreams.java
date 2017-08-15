/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.streams;

import java.util.function.Function;

import com.ibm.streamsx.topology.TStream;

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

        return forceString(stream.filter(tuple -> tuple.contains(term)));
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

        return forceString(stream.filter(tuple -> tuple.startsWith(term)));
    }
    
    private static TStream<String> forceString(TStream<String> stream) {
        stream = stream.asType(String.class);
        assert stream.getTupleType() == String.class;
        assert stream.getTupleClass() == String.class;
        return stream;
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
        return forceString(stream.map(Object::toString));
    }
}
