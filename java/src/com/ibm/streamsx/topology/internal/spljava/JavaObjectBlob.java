/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import com.ibm.streams.operator.types.Blob;

public class JavaObjectBlob implements Blob {

    private byte[] data;
    private final Object object;

    JavaObjectBlob(Object object) {
        this.object = object;
    }

    Object getObject() {
        return object;
    }

    @Override
    public long getLength() {
        if (data == null)
            serializeObject();
        return data.length;
    }

    @Override
    public ByteBuffer getByteBuffer() {
        if (data == null)
            serializeObject();
        return ByteBuffer.wrap(data);
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

        return null;
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
        return buf.put(data);
    }
    
    @Override
    public String toString() {
        return object.toString();
    }

    /************************/

    private void serializeObject() {

        try {
            AB baos = new AB();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.flush();
            oos.close();
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
