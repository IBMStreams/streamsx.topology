/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.topology.internal.spljava.Schemas;

public class SPLSchemas {

    private SPLSchemas() {
    }

    /**
     * SPL schema used by Tuple&lt;String> streams. Consists of a single
     * attribute of type {@code rstring string}.
     */
    public StreamSchema STRING = Schemas.STRING;
}
