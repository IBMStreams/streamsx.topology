/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;

/**
 * Provides the API for the conversion between a Java object and its SPL Tuple
 * representation.
 * 
 * @param <T>
 *            Type of the Java object.
 */
public abstract class SPLMapping<T> {

    static final StringMapping JavaString = new StringMapping();
    static final BlobMapping JavaBlob = new BlobMapping();
    static final XMLMapping JavaXML = new XMLMapping();

    private final StreamSchema schema;

    protected SPLMapping(StreamSchema schema) {
        this.schema = schema;
    }

    StreamSchema getSchema() {
        return schema;
    }
    public abstract Tuple convertTo(T tuple);

    public abstract T convertFrom(Tuple tuple);
}