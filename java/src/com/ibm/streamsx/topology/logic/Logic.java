/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.logic;

import java.util.List;

import com.ibm.streamsx.topology.function.BiFunction;
import com.ibm.streamsx.topology.function.Function;
import com.ibm.streamsx.topology.function.UnaryOperator;
import com.ibm.streamsx.topology.internal.logic.FirstOfSecondParameterIterator;

public class Logic {
    static final Object ZERO = Integer.valueOf(0);
    
    public static <T> UnaryOperator<T> identity() {
        return new Identity<>();
    }
    
    public static <T> Function<T,Object> notKeyed() {
        return new Function<T,Object>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object apply(T v) {              
                return ZERO;
            }          
        };
    }

    /**
     * Wrap a {@link BiFunction} instance that operators on a single value for
     * the second parameter (type {@code U}) as a {@link BiFunction} that takes
     * an {@code List}. Only the first value from the list will be passed to to
     * {@code logic}. If the list is empty, then {@code null} is passed.
     * 
     * @param logic
     *            Logic to be called.
     * @return Wrapper {@code Function2} that accepts an {@code List} of type
     *         {@code U} and passes the first value or {@code null} to {@code logic}.
     */
    public static <T, U, R> BiFunction<T, List<U>, R> first(
            final BiFunction<T, U, R> logic) {
        return new FirstOfSecondParameterIterator<T, U, R>(logic);
    }
}
