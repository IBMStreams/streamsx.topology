/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.json;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import com.ibm.streams.operator.StreamSchema;

public class JSONSchemas {

    private JSONSchemas() {
    }

    /**
     * SPL schema used by Tuple&lt;String> streams. Consists of a single
     * attribute of type {@code rstring __spl_js}.
     */
    public static StreamSchema JSON = getStreamSchema("tuple<rstring jsonString>");
}
