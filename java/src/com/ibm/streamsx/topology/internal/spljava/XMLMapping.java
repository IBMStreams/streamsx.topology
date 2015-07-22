/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.types.XML;

/**
 * SPL tuple for a TStream<XML> Uses the SPL schema Schemas.XML.
 * 
 */
class XMLMapping extends SPLMapping<XML> {

    // Singleton, as stateless.
    XMLMapping() {
        super(Schemas.XML);
    }

    @Override
    public Tuple convertTo(XML tuple) {
        if (tuple.isDefaultValue())
            return getSchema().getTuple();

        return getSchema().getTuple(new Object[] { tuple });
    }

    @Override
    public XML convertFrom(Tuple tuple) {
        return tuple.getXML(0);
    }
}
