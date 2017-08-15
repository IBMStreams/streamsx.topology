package com.ibm.streamsx.topology.spi.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

final class JavaSerializer implements TupleSerializer {
        
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    JavaSerializer() {
    }

    @Override
    public void serialize(Object tuple, OutputStream output) throws IOException {
        ObjectOutputStream oos = new ObjectOutputStream(output);
        oos.writeObject(tuple);
        oos.flush();
        oos.close();
    }

    @Override
    public Object deserialize(InputStream input) throws IOException, ClassNotFoundException {
        
        try (ObjectInputStream ois = new ObjectInputStream(input)) {
            return ois.readObject();
        }
    }

}
