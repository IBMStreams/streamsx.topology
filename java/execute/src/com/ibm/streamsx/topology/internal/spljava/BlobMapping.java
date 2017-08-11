/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.types.Blob;

/**
 * SPL tuple for a TStream<Blob> Uses the SPL schema Schemas.BLOB.
 * 
 */
class BlobMapping extends SPLMapping<Blob> {

    // Singleton, as stateless.
    BlobMapping() {
        super(Schemas.BLOB);
    }

    @Override
    public Tuple convertTo(Blob tuple) {
        if (tuple.getLength() == 0)
            return getSchema().getTuple();

        return getSchema().getTuple(new Object[] { tuple });
    }

    @Override
    public Blob convertFrom(Tuple tuple) {
        return tuple.getBlob(0);
    }
}
