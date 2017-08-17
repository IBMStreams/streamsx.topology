/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.functional;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class ObjectSchemas {
    
    public static final String STRING_SCHEMA = "tuple<rstring string>";
    public static final String BLOB_SCHEMA = "tuple<blob binary>";
    public static final String XML_SCHEMA = "tuple<xml document>";
    public static final String JAVA_OBJECT_SCHEMA = "tuple<blob __spl_jo>";
    public static final String JSON_SCHEMA = "tuple<rstring jsonString>";
    
    private static final Set<String> DIRECT_SCHEMA_CLASSES;
    static {
        Set<String> directSchemaClasses = new HashSet<>();
        directSchemaClasses.add(String.class.getName());
        directSchemaClasses.add("com.ibm.streams.operator.types.Blob");
        directSchemaClasses.add("com.ibm.streams.operator.types.XML");
        
        DIRECT_SCHEMA_CLASSES = Collections.unmodifiableSet(directSchemaClasses);
    }
    
    public static boolean usesDirectSchema(Type type) {
        if (type instanceof Class)
            return DIRECT_SCHEMA_CLASSES.contains(((Class<?>) type).getName());
        return false;
    }
    
    public static String getMappingSchema(Type tupleType) {
        if (tupleType instanceof Class) {

            if (String.class.equals(tupleType))
                return STRING_SCHEMA;

            Class<?> clazz = (Class<?>) tupleType;

            if (clazz.getName().equals("com.ibm.streams.operator.types.Blob"))
                return BLOB_SCHEMA;

            if (clazz.getName().equals("com.ibm.streams.operator.types.XML"))
                return XML_SCHEMA;
        }

        return JAVA_OBJECT_SCHEMA;
    }
    
    private static final String HASH_ATTR_SCHEMA = ", int32 __spl_hash>";
    
    public static String schemaWithHash(String schema) {
        switch (schema) {
        case STRING_SCHEMA:
        case JAVA_OBJECT_SCHEMA:
        case BLOB_SCHEMA:
        case XML_SCHEMA:
        case JSON_SCHEMA:
            return schema.replace(">", HASH_ATTR_SCHEMA);
        default:
            throw new IllegalStateException(schema);
        }
    }
}
