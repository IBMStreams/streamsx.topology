/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import java.io.IOException;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.types.Blob;
import com.ibm.streamsx.topology.spi.runtime.TupleSerializer;

/**
 * Mapping for a Java object as an SPL schema.
 * Like Java at runtime (the schema) we have no knowledge of
 * the actual type of the Java object, the info for
 *  generic parameter of TStream<T> is not needed. 
 */
class SPLJavaObject extends SPLMapping<Object> {

    /**
     * Attribute name for a schema with a serialized java object
     */
    public static final String SPL_JAVA_OBJECT = "__spl_jo";
    
    private final TupleSerializer serializer;

    SPLJavaObject(StreamSchema schema) {
        this(schema, TupleSerializer.JAVA_SERIALIZER);
    }
    
    SPLJavaObject(StreamSchema schema, TupleSerializer serializer) {
        super(schema);
        this.serializer = serializer;
    }

    @Override
    public Object convertFrom(Tuple tuple) {
        Blob blob = tuple.getBlob(0);

        if (blob instanceof JavaObjectBlob) {
            JavaObjectBlob jblob = (JavaObjectBlob) blob;
            return jblob.getObject();
        }
        
        try {
            return serializer.deserialize(blob.getInputStream());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Tuple convertTo(Object tuple) {

        JavaObjectBlob jblob = new JavaObjectBlob(serializer, tuple);
        return getSchema().getTuple(new Blob[] { jblob });
    }
}
