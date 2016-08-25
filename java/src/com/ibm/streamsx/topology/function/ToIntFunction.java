/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function;

import java.io.Serializable;

/**
 * A function that applies a computation to a tuple
 * and returns an {@code int} result.
 * <BR>
 * If an implementation also implements
 * {@code java.lang.AutoCloseable} then it will be
 * automatically closed when the application terminates.
 * @param <T>
 *            Type of the input to the function
 */
public interface ToIntFunction<T> extends Serializable {
    
    /**
     * Apply the function to the {@code tuple} and return an {@code int}.
     * 
     * @param tuple Tuple to be tested.
     * @return int result of the function.
     */
    int applyAsInt(T tuple);
}
