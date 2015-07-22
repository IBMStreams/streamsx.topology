/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.types.RString;

/**
 * SPL tuple for a TStream<String> Uses the SPL schema Schemas.STRING.
 * 
 */
class StringMapping extends SPLMapping<String> {

    // Singleton, as stateless.
    StringMapping() {
        super(Schemas.STRING);
    }

    @Override
    public Tuple convertTo(String tuple) {
        if (tuple.isEmpty())
            return getSchema().getTuple();

        return getSchema().getTuple(new Object[] { new RString(tuple) });
    }

    @Override
    public String convertFrom(Tuple tuple) {
        return tuple.getString(0);
    }
}
