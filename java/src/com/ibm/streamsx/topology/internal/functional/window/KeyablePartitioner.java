/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional.window;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.window.StreamWindowPartitioner;
import com.ibm.streamsx.topology.internal.spljava.SPLMapping;
import com.ibm.streamsx.topology.tuple.Keyable;

public class KeyablePartitioner implements
        StreamWindowPartitioner<Tuple, Object> {

    private SPLMapping<? extends Keyable<?>> mapping;

    public KeyablePartitioner(SPLMapping<? extends Keyable<?>> mapping) {
        this.mapping = mapping;
    }

    @Override
    public Object getPartition(Tuple tuple) {
        return mapping.convertFrom(tuple).getKey();
    }
}
