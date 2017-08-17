/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.spl;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streamsx.topology.internal.functional.ObjectSchemas;
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
    StreamSchema STRING = getStreamSchema(ObjectSchemas.STRING_SCHEMA);
    
    /**
     * SPL schema used by
     * {@code TStream<com.ibm.streams.operator.types.XML> streams}.
     * Consists of a single attribute of type {@code xml document}.
     */
    StreamSchema XML = getStreamSchema(ObjectSchemas.XML_SCHEMA);
    
    /**
     * SPL schema used by
     * {@code TStream<com.ibm.streams.operator.types.Blob>} streams.
     * Consists of a single attribute of type {@code blob binary}.
     */
    StreamSchema BLOB = getStreamSchema(ObjectSchemas.BLOB_SCHEMA);
    
    /**
     * SPL schema used to publish and subscribe to 
     * {@code TStream<JSONObject>} streams. Used to interchange
     * streams of JSON objects between applications.
     * Consists of a single attribute of type {@code rstring jsonString}.
     */
    StreamSchema JSON = JSONSchemas.JSON;
}
