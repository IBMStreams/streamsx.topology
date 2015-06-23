/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streams.operator.types.XML;

public class Schemas {

    public static final StreamSchema STRING = getStreamSchema("tuple<rstring __spl_js>");
    public static final StreamSchema BLOB = getStreamSchema("tuple<blob __spl_jb>");
    public static final StreamSchema XML = getStreamSchema("tuple<xml __spl_jx>");
    

    public StreamSchema getSPLSchema(Class<?> tupleClass) {

        if (String.class.equals(tupleClass))
            return STRING;
        if (Blob.class.equals(tupleClass))
            return BLOB;
        if (XML.class.equals(tupleClass))
            return XML;

        throw new IllegalArgumentException(tupleClass.getName());
    }

    @SuppressWarnings("unchecked")
    public static <T> SPLMapping<T> getSPLMapping(Class<T> tupleClass) {

        if (String.class.equals(tupleClass)) {
            return (SPLMapping<T>) SPLMapping.JavaString;
        }
        if (Blob.class.equals(tupleClass)) {
            return (SPLMapping<T>) SPLMapping.JavaBlob;
        }
        if (XML.class.equals(tupleClass)) {
            return (SPLMapping<T>) SPLMapping.JavaXML;
        }

        return SPLJavaObject.createMappping(tupleClass);
    }

    public static SPLMapping<?> getSPLMapping(StreamSchema schema) {

        if (STRING.equals(schema)) {
            return SPLMapping.JavaString;
        }
        if (BLOB.equals(schema)) {
            return SPLMapping.JavaBlob;
        }
        if (XML.equals(schema)) {
            return SPLMapping.JavaXML;
        }

        Attribute attr0 = schema.getAttribute(0);

        if (attr0.getType().getMetaType() == Type.MetaType.BLOB) {
            if (attr0.getName().startsWith(SPLJavaObject.SPL_JAVA_PREFIX))
                return SPLJavaObject.getMapping(schema);
        }

        return new SPLTuple(schema);

        // throw new UnsupportedOperationException(
        // "Mapping not defined:" + schema.getLanguageType());
    }

}
