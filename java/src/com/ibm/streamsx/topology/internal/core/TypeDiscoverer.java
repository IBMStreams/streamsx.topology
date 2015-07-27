/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Supplier;

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
        
        ParameterizedType pt = findParameterizedType(Function.class, function);
        if (pt != null)  {
            return determineClassFromArgument(pt, 1);
        }
        throw nullTupleClass();
    }
    
    public static <T> Type determineStreamTypeIterable(Function<?,Iterable<T>> function, Type tupleType) {

        if (tupleType != null)
            return tupleType;
        
        ParameterizedType pt = findParameterizedType(Function.class, function);
        if (pt != null)  {
            Type iterator = determineClassFromArgument(pt, 1);
            ParameterizedType iterpt;
            if (iterator instanceof Class)
                iterpt = findParameterizedType(Iterable.class, (Class<?>) iterator);
            else if (iterator instanceof ParameterizedType)
                iterpt = (ParameterizedType) iterator;
            else
                throw nullTupleClass();
            
            if (!Iterable.class.equals(iterpt.getRawType())) {
                throw nullTupleClass();
            }
            
            return iterpt.getActualTypeArguments()[0];
        }
        throw nullTupleClass();
    }
    
    public static Type determineStreamType(Supplier<?> function, Type tupleType) {
        
        if (tupleType != null)
            return tupleType;
        
        ParameterizedType pt = findParameterizedType(Supplier.class, function);
        if (pt != null)  {
            return determineClassFromArgument(pt, 0);
        }
        throw nullTupleClass();
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
    
    private static Type determineClassFromArgument(ParameterizedType pt, int arg) {
        Type argType = pt.getActualTypeArguments()[arg];
        if (argType instanceof Class) {
            return argType;
        }
        if (argType instanceof ParameterizedType)
            return argType;
        throw nullTupleClass();
    }

    public static String getTupleName(Type tupleType) {
        if (tupleType instanceof Class)
            return ((Class<?>) tupleType).getSimpleName();
        return "Object";
    }
}
