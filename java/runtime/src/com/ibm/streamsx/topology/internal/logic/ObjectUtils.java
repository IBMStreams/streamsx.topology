/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

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
    
    private static final Set<Class<?>> immutableClasses = new HashSet<>();
    static {
        immutableClasses.add(String.class);
        
        immutableClasses.add(Boolean.class);
        
        immutableClasses.add(Byte.class);
        immutableClasses.add(Short.class);
        immutableClasses.add(Integer.class);
        immutableClasses.add(Long.class);
        
        immutableClasses.add(BigInteger.class);
        immutableClasses.add(BigDecimal.class);
        
        immutableClasses.add(Float.class);
        immutableClasses.add(Double.class);
        
        immutableClasses.add(File.class);
        
        immutableClasses.add(Character.class);
        
        immutableClasses.add(Locale.class);
        immutableClasses.add(UUID.class);
        
    }
    
    /**
     * See if the functional logic is stateful.
     * 
     * Logic is stateful if:
     *   Has a non-final instance field.
     *   Has a final instance field that is not a primitive.
     *   
     * @param logic
     * @return
     */
    public static boolean isImmutable(Serializable logic) {
        return isImmutable(logic.getClass());
    }
    
    public static boolean isImmutable(Class<?> clazz) {
               
        do {
               Field[] fields = clazz.getDeclaredFields();
               for (Field field : fields) {
                   if (Modifier.isStatic(field.getModifiers()))
                       continue;
                   
                   if (Modifier.isTransient(field.getModifiers()))
                       continue;
                   
                   if (!Modifier.isFinal(field.getModifiers()))
                       return false;
                   
                   if (field.getType().isPrimitive())
                       continue; 
                   
                   if (immutableClasses.contains(field.getType()))
                       continue;
                   
                   if (field.getType().isEnum()) {
                       if (isImmutable(field.getType()))
                           continue;
                   }
                   
                   return false;
               }
               
               clazz = clazz.getSuperclass();
               
        } while (!Object.class.equals(clazz));
        
        return true;
    }
}
