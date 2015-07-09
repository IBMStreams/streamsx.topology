/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

import java.io.Serializable;

/**
 * A function that is passed two arguments and returns a value.
 *
 * @deprecated Replaced by
 * {@link com.ibm.streamsx.topology.function.BiFunction}
 */
@Deprecated
public interface BiFunction<T1, T2, R> extends
   com.ibm.streamsx.topology.function.BiFunction<T1,T2,R> { }
