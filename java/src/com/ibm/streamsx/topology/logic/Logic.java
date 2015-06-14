/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.logic;

import com.ibm.streamsx.topology.function7.BiFunction;
import com.ibm.streamsx.topology.internal.logic.FirstOfSecondParameterIterator;

public class Logic {

    /**
     * Wrap a {@link BiFunction} instance that operators on a single value for
     * the second parameter (type {@code U}) as a {@link BiFunction} that takes
     * an iterator. Only the first value from the iterator will be passed to to
     * {@code logic}.
     * 
     * @param logic
     *            Logic to be called.
     * @return Wrapper {@code Function2} that accepts an iterator of type
     *         {@code U} and passes the first value, if any to {@code logic}.
     */
    public static <T, U, R> BiFunction<T, Iterable<U>, R> first(
            final BiFunction<T, U, R> logic) {
        return new FirstOfSecondParameterIterator<T, U, R>(logic);
    }
}
