/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

import java.io.Serializable;

/**
 * A function that tests a tuple.
 * 
 * @param <T>
 *            Type of the input to the predicate
 */
public interface Predicate<T> extends Serializable {
    
    /**
     * Test {@code tuple} against this predicate.
     * 
     * @param tuple Tuple to be tested.
     * @return True if the tuple passed this predicate, false otherwise.
     */
    boolean test(T tuple);
}
