/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function;

import java.io.Serializable;

/**
 * A function that supplies a value.
 * <BR>
 * If an implementation also implements
 * {@code java.lang.AutoCloseable} then it will be
 * automatically closed when the application terminates.
 * 
 * @param <T>
 *            Type of the return value.
 */
public interface Supplier<T> extends Serializable {

    T get();
}
