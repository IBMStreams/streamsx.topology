/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import java.io.IOException;
import java.io.ObjectInputStream;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.types.Blob;

/**
 * Mapping for a Java object as an SPL schema.
 * Like Java at runtime (the schema) we have no knowledge of
 * the actual type of the Java object, the info for
 *  generic parameter of TStream<T> is not needed. 
 */
class SPLJavaObject<T> extends SPLMapping<T> {

    public static final String SPL_JAVA_PREFIX = "__spl_j";
    
    public static final String SPL_JAVA_OBJECT = SPL_JAVA_PREFIX + "object";

    private final Class<T> tupleClass;

    static <T> SPLMapping<T> createMappping(Class<T> tupleClass) {       
        return new SPLJavaObject<T>(Schemas.JAVA_OBJECT, tupleClass);
    }

    protected SPLJavaObject(StreamSchema schema, Class<T> tupleClass) {
        super(schema, tupleClass);
        this.tupleClass = tupleClass;
    }

    @Override
    public T convertFrom(Tuple tuple) {
        Blob blob = tuple.getBlob(0);

        if (blob instanceof JavaObjectBlob) {
            JavaObjectBlob jblob = (JavaObjectBlob) blob;
            return tupleClass.cast(jblob.getObject());
        }

        try {

            ObjectInputStream ois = new ObjectInputStream(blob.getInputStream());

            return tupleClass.cast(ois.readObject());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Tuple convertTo(T tuple) {

        JavaObjectBlob jblob = new JavaObjectBlob(tuple);
        return getSchema().getTuple(new Blob[] { jblob });
    }
}
