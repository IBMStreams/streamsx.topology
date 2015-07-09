/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

/**
 * Specialization of {@link Function} where the return type is the same as the
 * argument type.
 *
 * @deprecated Replaced by
 * {@link com.ibm.streamsx.topology.function.UnaryOperator}
 */
@Deprecated
public interface UnaryOperator<T> extends
   com.ibm.streamsx.topology.function.UnaryOperator<T>, Function<T,T> { }
