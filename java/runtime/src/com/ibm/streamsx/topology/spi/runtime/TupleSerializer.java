/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.spi.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;


public interface TupleSerializer extends Serializable {
    
    TupleSerializer JAVA_SERIALIZER = new JavaSerializer();
    
    void serialize(Object tuple, OutputStream output) throws IOException;
    
    Object deserialize(InputStream input) throws IOException, ClassNotFoundException;
}
