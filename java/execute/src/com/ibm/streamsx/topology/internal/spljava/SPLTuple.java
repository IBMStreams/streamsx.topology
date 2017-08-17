/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;

/**
 * Mapping class for SPLStreams, where T of TStream<T> is the actual Tuple.
 * 
 */
class SPLTuple extends SPLMapping<Tuple> {

    SPLTuple(StreamSchema schema) {
        super(schema);
    }

    @Override
    public Tuple convertFrom(Tuple tuple) {
        return tuple;
    }

    @Override
    public Tuple convertTo(Tuple tuple) {
        return tuple;
    }
}
