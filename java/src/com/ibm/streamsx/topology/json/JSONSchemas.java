/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.json;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import com.ibm.streams.operator.StreamSchema;

/**
 * SPL schema for JSON.
 *
 */
public class JSONSchemas {

    private JSONSchemas() {
    }

    /**
     * SPL schema used for SPL streams with JSON data. Consists of a single
     * attribute of type {@code rstring jsonString}.
     */
    public static StreamSchema JSON = getStreamSchema("tuple<rstring jsonString>");
}
