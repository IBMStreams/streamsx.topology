/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.XML;

public class Schemas {
    @SuppressWarnings("unused")
    private static final Schemas forCoverage = new Schemas();
    
    public static final StreamSchema STRING = getStreamSchema("tuple<rstring string>");
    public static final StreamSchema BLOB = getStreamSchema("tuple<blob binary>");
    public static final StreamSchema XML = getStreamSchema("tuple<xml document>");
    public static final StreamSchema JAVA_OBJECT = getStreamSchema("tuple<blob " + SPLJavaObject.SPL_JAVA_OBJECT + ">");
    
    
    private static final Set<Class<?>> DIRECT_SCHEMA_CLASSES;
    static {
        Set<Class<?>> directSchemaClasses = new HashSet<>();
        directSchemaClasses.add(String.class);
        directSchemaClasses.add(Blob.class);
        directSchemaClasses.add(XML.class);
        
        DIRECT_SCHEMA_CLASSES = Collections.unmodifiableSet(directSchemaClasses);
    }
    
    private Schemas() { }
    
    public static boolean usesDirectSchema(Type type) {
        return DIRECT_SCHEMA_CLASSES.contains(type);
    }
    

    public StreamSchema getSPLSchema(Class<?> tupleClass) {

        if (String.class.equals(tupleClass))
            return STRING;
        if (Blob.class.equals(tupleClass))
            return BLOB;
        if (XML.class.equals(tupleClass))
            return XML;

        throw new IllegalArgumentException(tupleClass.getName());
    }

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
