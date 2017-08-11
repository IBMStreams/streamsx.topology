/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.ibm.streams.operator.types.Blob;
import com.ibm.streamsx.topology.spi.runtime.TupleSerializer;

public class JavaObjectBlob implements Blob {

    private final TupleSerializer serializer;
    private byte[] data;
    private int len;
    private final Object object;

    JavaObjectBlob(TupleSerializer serializer, Object object) {
        this.serializer = serializer;
        this.object = object;
    }

    Object getObject() {
        return object;
    }

    @Override
    public long getLength() {
        if (data == null)
            serializeObject();
        return len;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        if (data == null)
            serializeObject();
        return ByteBuffer.wrap(data, 0, len);
    }

    @Override
    public ByteBuffer getByteBuffer(long position, int length) {
        if (data == null)
            serializeObject();
        return ByteBuffer.wrap(data, (int) position, length);
    }

    @Override
    public InputStream getInputStream() {
        if (data == null)
            serializeObject();

        return new ByteArrayInputStream(data, 0, len);
    }

    @Override
    public byte[] getData() {
        if (data == null)
            serializeObject();
        return data.clone();
    }

    @Override
    public ByteBuffer put(ByteBuffer buf) {
        if (data == null)
            serializeObject();
        return buf.put(data, 0, len);
    }
    
    @Override
    public String toString() {
        return object.toString();
    }

    /************************/

    private synchronized void serializeObject() {

        try {
            AB baos = new AB();
            
            serializer.serialize(object, baos);
            len = baos.size();
            data = baos.data();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class AB extends ByteArrayOutputStream {
        AB() {
            super(1024);
        }

        byte[] data() {
            return buf;
        }
    }

}
