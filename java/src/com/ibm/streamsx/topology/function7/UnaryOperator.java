/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

/**
 * Specialization of {@link Function} where the return type is the same as the
 * argument type.
 * 
 * @param <T>
 *            Type of the argument and return
 */
public interface UnaryOperator<T> extends Function<T, T> {

}
