/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

import java.io.Serializable;

/**
 * A function that supplies a value.
 * 
 * @param <T>
 *            Type of the return value.
 */
public interface Supplier<T> extends Serializable {

    T get();
}
