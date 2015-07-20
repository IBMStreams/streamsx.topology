/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.spljava;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.regex.Pattern;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.types.Blob;

class SPLJavaObject<T> extends SPLMapping<T> {

    public static final String SPL_JAVA_PREFIX = "__spl_j";
    
    public static final String SPL_JAVA_PREFIX_SIMPLE = SPL_JAVA_PREFIX + "o_";
    public static final String SPL_JAVA_PREFIX_ENCODED = SPL_JAVA_PREFIX + "ou_";
    
    public static final String SPL_JAVA_OBJECT = SPL_JAVA_PREFIX + "object";

    private final Class<T> tupleClass;

    private static boolean onlyASCII(String className) {
        return Pattern.matches("^[\\dA-Za-z\\.]*$", className);
    }
    static String classNameToSPLAttributeName(Class<?> tupleClass) {
        return classNameToSPLAttributeName(tupleClass.getName());
    }
    
    private static String classNameToSPLAttributeName(String className) {
        
        if (onlyASCII(className))
            return simpleJavaAttributeName(className);
        
        return encodedJavaAttributeName(className);
    }
    
    /**
     * Create a simple attribute name for a class name
     * that only contains a-Z,A-Z,0-9 and package separators (.).
     * 
     * In this case just replace the dots by _ and prefix
     * with SPL_JAVA_PREFIX_SIMPLE.
     * 
     * This keeps the class readable in the SPL schema.
     */
    private static String simpleJavaAttributeName(String className) {
        assert onlyASCII(className);
        assert !className.contains("_");
        
       String attrName = className.replace('.', '_');
        
       return SPL_JAVA_PREFIX_SIMPLE.concat(attrName);
    }
    
    private static String simpleSPLAttributeNameToClassName(String attrName) {
        assert attrName.startsWith(SPL_JAVA_PREFIX_SIMPLE);
        
        attrName = attrName.substring(SPL_JAVA_PREFIX_SIMPLE.length());
        attrName = attrName.replace('_', '.');
        
        assert onlyASCII(attrName);
        
        return attrName;    
    }
    /*
    * Create an encoded attribute name for a class name
    * that may contain any characters. Since SPL only
    * supports ASCII letters, numbers and underscore
    * in identifiers, we need to escape any other character
    * in the class name. We use underscore as the escape
    * character:
    * 
    * __ (two underscores) - escape for an underscore.
    * _p  - package separate (dot, '.')
    * _uxxxx xxxx is the unicode escape sequence in hex.
    * 
    * The attribute name is prepended by SPL_JAVA_PREFIX_ENCODED 
    * 
    */
    private static final String ZEROS = "000";
   private static String encodedJavaAttributeName(String className) {
       
       StringBuilder sb = new StringBuilder(className.length()*2);
       sb.append(SPL_JAVA_PREFIX_ENCODED);
       
       for (int i = 0; i < className.length(); i++) {
           final char c = className.charAt(i);
           
           // Simple ASCII
           if ((c >= 'a' && c <= 'z') ||
              (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9')) {
               sb.append(c);
               continue;
           }
           
           if (c == '.') {
               sb.append("_p");
               continue;
           }
           
           if (c == '_') {
               sb.append("__");
               continue;
           }
           
           sb.append("_u");
           String hex = Integer.toHexString(c);
           sb.append(ZEROS.substring(0, 4-hex.length()));
           sb.append(hex);
       }
       
      return sb.toString();
   }
   private static String encodedSPLAttributeNameToClassName(String attrName) {
       
       assert attrName.startsWith(SPL_JAVA_PREFIX_ENCODED);
       
       StringBuilder sb = new StringBuilder(attrName.length());
       
       for (int i = SPL_JAVA_PREFIX_ENCODED.length(); i < attrName.length(); ) {
           final char c = attrName.charAt(i++);
           
           if (c != '_') {
               sb.append(c);
               continue;
           }
           
           char ins = attrName.charAt(i++);
           
           if (ins == 'p') {
               sb.append('.');
               continue;
           }
           
           if (ins == '_') {
               sb.append('_');
               continue;
           }
           
           if (ins == 'u') {
               String hex = attrName.substring(i, i+4);
               i+= 4;
               char hc = (char) Integer.parseInt(hex, 16);
               sb.append(hc);
               continue;
           }
           
           throw new IllegalArgumentException(attrName);
       }
       
      return sb.toString();

   }

    static String SPLAttributeNameToClassName(String attrName) {
        if (attrName.startsWith(SPL_JAVA_PREFIX_SIMPLE))
            return simpleSPLAttributeNameToClassName(attrName);
        
        return encodedSPLAttributeNameToClassName(attrName);
    }

    static <T> SPLMapping<T> createMappping(Class<T> tupleClass) {

        //String splName = classNameToSPLAttributeName(tupleClass);
        //StreamSchema schema = Type.Factory.getStreamSchema("tuple<blob "
        //        + splName + ">");
        StreamSchema schema = Schemas.JAVA_OBJECT;
        
        return new SPLJavaObject<T>(schema, tupleClass);
    }

    static SPLMapping<Object> getMapping(StreamSchema schema) {
        assert schema.getAttributeCount() == 1;

        Attribute blobAttr = schema.getAttribute(0);
        assert blobAttr.getType().getMetaType() == Type.MetaType.BLOB;

        String attrName = blobAttr.getName();
        assert attrName.startsWith(SPL_JAVA_PREFIX);

        String javaClassName = SPLAttributeNameToClassName(attrName);

        try {
            @SuppressWarnings("unchecked")
            Class<Object> tupleClass = (Class<Object>) Class
                    .forName(javaClassName);
            return new SPLJavaObject<Object>(schema, tupleClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

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

        /*
         * 
         * try { // baos.reset(); AB baos = new AB(); ObjectOutputStream oos =
         * new ObjectOutputStream(baos); oos.writeObject(tuple); oos.flush();
         * oos.close(); Blob objectAsBlob = ValueFactory.newBlob(baos.data(), 0,
         * baos.size()); return getSchema().getTuple(new Blob[] { objectAsBlob
         * });
         * 
         * } catch (IOException e) { throw new RuntimeException(e); }
         */
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
