/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.topology.internal.spljava.Schemas;
import com.ibm.streamsx.topology.json.JSONSchemas;

/**
 * Interchangeable SPL types.
 * SPL tuple types that can be used to interchange
 * streams between IBM Streams applications implemented
 * in different languages, such as SPL and Java.
 *
 */
public interface SPLSchemas {
    /**
     * SPL schema used by
     * {@code TStream<String>} streams. Consists of a single
     * attribute of type {@code rstring string}.
     */
    StreamSchema STRING = Schemas.STRING;
    
    /**
     * SPL schema used by
     * {@code TStream<com.ibm.streams.operator.types.XML> streams}.
     * Consists of a single attribute of type {@code xml document}.
     */
    StreamSchema XML = Schemas.XML;
    
    /**
     * SPL schema used by
     * {@code TStream<com.ibm.streams.operator.types.Blob>} streams.
     * Consists of a single attribute of type {@code blob binary}.
     */
    StreamSchema BLOB = Schemas.BLOB;
    
    /**
     * SPL schema used to publish and subscribe to 
     * {@code TStream<JSONObject>} streams. Used to interchange
     * streams of JSON objects between applications.
     * Consists of a single attribute of type {@code rstring jsonString}.
     */
    StreamSchema JSON = JSONSchemas.JSON;
}
