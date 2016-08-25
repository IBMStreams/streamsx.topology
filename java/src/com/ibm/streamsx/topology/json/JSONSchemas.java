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
public interface JSONSchemas {

    /**
     * IBM Streams schema used for streams with JSON data. Consists of a single
     * attribute of type {@code rstring jsonString}.
     */
    StreamSchema JSON = getStreamSchema("tuple<rstring jsonString>");
}
