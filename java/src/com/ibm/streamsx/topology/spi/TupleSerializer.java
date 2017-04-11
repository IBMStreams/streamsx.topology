package com.ibm.streamsx.topology.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.ibm.streamsx.topology.internal.spljava.JavaSerializer;

public interface TupleSerializer {
    
    TupleSerializer JAVA_SERIALIZER = new JavaSerializer();
    
    void serialize(Object tuple, OutputStream output) throws IOException;
    
    Object deserialize(InputStream input) throws IOException, ClassNotFoundException;
}
