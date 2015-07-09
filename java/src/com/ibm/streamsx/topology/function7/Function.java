/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

import java.io.Serializable;

/**
 * A function that is passed an argument and returns a value.
 *
 * @deprecated Replaced by
 * {@link com.ibm.streamsx.topology.function.Function}
 */
@Deprecated
public interface Function<T, R> extends 
   com.ibm.streamsx.topology.function.Function<T,R> { }
