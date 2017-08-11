/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;
import static com.ibm.streamsx.topology.internal.core.ObjectUtils.deserializeLogic;

import java.lang.reflect.Type;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.XML;
import com.ibm.streamsx.topology.internal.core.ObjectSchemas;
import com.ibm.streamsx.topology.spi.TupleSerializer;

public class Schemas {
    @SuppressWarnings("unused")
    private static final Schemas forCoverage = new Schemas();
    
    public static final StreamSchema STRING = getStreamSchema(ObjectSchemas.STRING_SCHEMA);
    public static final StreamSchema BLOB = getStreamSchema(ObjectSchemas.BLOB_SCHEMA);
    public static final StreamSchema XML = getStreamSchema(ObjectSchemas.XML_SCHEMA);
    public static final StreamSchema JAVA_OBJECT = getStreamSchema(ObjectSchemas.JAVA_OBJECT_SCHEMA);
    
    /**
     * Return the SPL schema that will be used at runtime
     * to hold the java object tuple.
     */
    public static StreamSchema getSPLMappingSchema(Type tupleType) {

        if (String.class.equals(tupleType)) {
            return STRING;
        }
        if (Blob.class.equals(tupleType)) {
            return BLOB;
        }
        if (XML.class.equals(tupleType)) {
            return XML;
        }

        return JAVA_OBJECT;
    }
    
    public static SPLMapping<?> getObjectMapping(String tupleSerializer) throws ClassNotFoundException {
        TupleSerializer serializer = (TupleSerializer) deserializeLogic(tupleSerializer);
        return new SPLJavaObject(JAVA_OBJECT, serializer);
    }

    public static SPLMapping<?> getSPLMapping(StreamSchema schema) {

        if (STRING.equals(schema)) {
            return SPLMapping.JavaString;
        }
        if (JAVA_OBJECT.equals(schema)) {
            return new SPLJavaObject(schema);
        }
        if (BLOB.equals(schema)) {
            return SPLMapping.JavaBlob;
        }
        if (XML.equals(schema)) {
            return SPLMapping.JavaXML;
        }

        return new SPLTuple(schema);
    }

}
