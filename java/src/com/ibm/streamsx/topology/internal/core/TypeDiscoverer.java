/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.UnaryOperator;

/**
 * Methods to determine the stream Java type from the logic type passed in.
 */
public class TypeDiscoverer {
    
    private static IllegalArgumentException nullTupleClass() {
        return new IllegalArgumentException("The tuple's class type must be known!");
    }

    /**
     * 
     */
    public static Type determineStreamType(Function<?,?> function, Type tupleType) {

        if (tupleType != null)
            return tupleType;
        
        if (function instanceof UnaryOperator)
            return determineStreamTypeFromFunctionArg(UnaryOperator.class, 0, function);
        
        return determineStreamTypeFromFunctionArg(Function.class, 1, function);
    }
    
    public static Type determineStreamTypeFromFunctionArg(Class<?> objectInterface, int arg, Object function) {
        
        ParameterizedType pt = findParameterizedType(objectInterface, function);
        if (pt != null)  {
            return pt.getActualTypeArguments()[arg];
        }
        return null;
    }
    
    public static <T> Type determineStreamTypeNested(Class<?> objectInterface, int arg, Class<?> nestedInterface, Object function) {
        
        ParameterizedType pt = findParameterizedType(objectInterface, function);
        if (pt != null)  {
            Type nestedArg = pt.getActualTypeArguments()[arg];
            ParameterizedType nestedPt;
            if (nestedArg instanceof Class)
                nestedPt = findParameterizedType(nestedInterface, (Class<?>) nestedArg);
            else if (nestedArg instanceof ParameterizedType)
                nestedPt = (ParameterizedType) nestedArg;
            else
                return Object.class;
            
            if (nestedPt == null || !nestedInterface.equals(nestedPt.getRawType())) {
                return Object.class;
            }
            
            return nestedPt.getActualTypeArguments()[0];
        }
        return Object.class;
    }
    
    public static Type determineStreamType(Supplier<?> function, Type tupleType) {
        
        if (tupleType != null)
            return tupleType;
        
        return determineStreamTypeFromFunctionArg(Supplier.class, 0, function);
    }
    
    private static ParameterizedType findParameterizedType(Class<?> functionInterface, Object function) {
        
        assert functionInterface.isInstance(function);

        Class<?> type = function.getClass();
        
        return findParameterizedType(functionInterface, type);
    }
    
    private static ParameterizedType findParameterizedType(Class<?> functionInterface, Class<?> type) {
        
        assert functionInterface.isAssignableFrom(type);
        
        do {
            for (Type ty : type.getGenericInterfaces()) {
                if (ty instanceof ParameterizedType) {
                    ParameterizedType pt = (ParameterizedType) ty;
                    if (functionInterface.equals(pt.getRawType())) {
                        return pt;
                    }
                }
            }
            type = type.getSuperclass();
        } while (!Object.class.equals(type));
        return null;
    }

    public static String getTupleName(Type tupleType) {
        if (tupleType instanceof Class)
            return ((Class<?>) tupleType).getSimpleName();
        return "Object";
    }
}
