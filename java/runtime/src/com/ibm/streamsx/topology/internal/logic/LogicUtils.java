/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

public class LogicUtils {
    
    public static String functionName(Object function) {
        
        while (function instanceof WrapperFunction)
            function = ((WrapperFunction) function).getWrappedFunction();

        return function.getClass().getSimpleName();
    }

}
