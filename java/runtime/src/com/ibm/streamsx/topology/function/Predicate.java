/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function;

import java.io.Serializable;

/**
 * A function that tests a tuple.
 * <BR>
 * If an implementation also implements
 * {@code java.lang.AutoCloseable} then it will be
 * automatically closed when the application terminates.
 * @param <T>
 *            Type of the input to the predicate
 */
@FunctionalInterface
public interface Predicate<T> extends Serializable {
    
    /**
     * Test {@code tuple} against this predicate.
     * 
     * @param tuple Tuple to be tested.
     * @return True if the tuple passed this predicate, false otherwise.
     */
    boolean test(T tuple);
}
