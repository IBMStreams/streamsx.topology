/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

import java.io.Serializable;

/**
 * A function that takes an argument and returns nothing.
 * 
 * @param <T>
 *            Type of the first (and only) argument
 */
public interface Consumer<T> extends Serializable {
    void accept(T v);
}
