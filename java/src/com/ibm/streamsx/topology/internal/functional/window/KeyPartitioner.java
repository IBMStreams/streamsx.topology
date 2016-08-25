/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindowPartitioner;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;

/**
 * Partition a tuple for a window using a function.
 * 
 */
public class KeyPartitioner implements
        StreamWindowPartitioner<Tuple,Object> {

    private final SPLMapping<Object> mapping;
    private final Function<Object,Object> keyGetter;

    public KeyPartitioner(SPLMapping<Object> mapping, Function<Object,Object> keyGetter) {
        this.mapping = mapping;
        this.keyGetter = keyGetter;
    }

    @Override
    public Object getPartition(Tuple tuple) {
        return keyGetter.apply(mapping.convertFrom(tuple));
    }
}
