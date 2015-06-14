/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

import java.io.Serializable;

/**
 * A function that is passed an argument and returns a value.
 * 
 * @param <T>
 *            Type of the first (and only) argument
 * @param <R>
 *            Type of the return value.
 */
public interface Function<T, R> extends Serializable {
    R apply(T v);
}
