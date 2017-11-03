/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import java.util.function.BiFunction;

import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.Predicate;
import com.ibm.streamsx.topology.function.Supplier;
import com.ibm.streamsx.topology.function.ToIntFunction;
import com.ibm.streamsx.topology.function.UnaryOperator;

public class LogicUtils {
    
    private static final Class<?>[] FUNCTIONAL_CLASSES = 
        new Class<?>[] {
        UnaryOperator.class,
        Function.class,
        Predicate.class,
        Consumer.class,
        Supplier.class,
        ToIntFunction.class,
        BiFunction.class,
        };
    
    public static String functionName(Object function) {
        
        while (function instanceof WrapperFunction)
            function = ((WrapperFunction) function).getWrappedFunction();
        
        Class<?> fc = function.getClass();
        if (fc.isSynthetic() || fc.isAnonymousClass()) {
            String type = fc.isSynthetic() ? "Lambda" : "Anonymous";
            
            if (fc.isAnonymousClass() && !fc.getSuperclass().equals(Object.class)) {
                return fc.getSuperclass().getSimpleName() + type;
            }
                    
            for (Class<?> efc : FUNCTIONAL_CLASSES) {
                if (efc.isInstance(function)) {
                    return efc.getSimpleName() + type;
                }
            }
            return type;
        }
            

        return function.getClass().getSimpleName();
    }

}
