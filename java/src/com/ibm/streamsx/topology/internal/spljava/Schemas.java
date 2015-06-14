/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import static com.ibm.streams.operator.Type.Factory.getStreamSchema;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Type;

public class Schemas {

    public static final StreamSchema STRING = getStreamSchema("tuple<rstring __spl_js>");

    public static final StreamSchema INT32 = getStreamSchema("tuple<int32 __spl_ji>");

    public static final StreamSchema INT64 = getStreamSchema("tuple<int64 __spl_jl>");

    public StreamSchema getSPLSchema(Class<?> tupleClass) {

        if (String.class.equals(tupleClass))
            return STRING;

        throw new IllegalArgumentException(tupleClass.getName());
    }

    @SuppressWarnings("unchecked")
    public static <T> SPLMapping<T> getSPLMapping(Class<T> tupleClass) {

        if (String.class.equals(tupleClass)) {
            return (SPLMapping<T>) SPLMapping.JavaString;
        }

        return SPLJavaObject.createMappping(tupleClass);
    }

    public static SPLMapping<?> getSPLMapping(StreamSchema schema) {

        if (STRING.equals(schema)) {
            return SPLMapping.JavaString;
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
