/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function;

/**
 * Specialization of {@link Function} where the return type is the same as the
 * argument type.
 * <BR>
 * If an implementation also implements
 * {@code java.lang.AutoCloseable} then it will be
 * automatically closed when the application terminates.
 * 
 * @param <T>
 *            Type of the argument and return
 */
public interface UnaryOperator<T> extends Function<T, T> {

}
