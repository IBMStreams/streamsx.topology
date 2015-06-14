/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.functional;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.xml.bind.DatatypeConverter;

public class ObjectUtils {

    public static String serializeLogic(Serializable logic) {

        try {
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bao);
            oos.writeObject(logic);
            oos.flush();
            return DatatypeConverter.printBase64Binary(bao.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Object deserializeLogic(String logicString)
            throws ClassNotFoundException {
        byte[] data = DatatypeConverter.parseBase64Binary(logicString);

        try {
            ObjectInputStream ois = new ObjectInputStream(
                    new ByteArrayInputStream(data));
            return ois.readObject();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
