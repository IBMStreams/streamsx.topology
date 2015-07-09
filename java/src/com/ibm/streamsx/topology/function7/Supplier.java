/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

import java.io.Serializable;

/**
 * A function that supplies a value.
 *
 * @deprecated Replaced by
 * {@link com.ibm.streamsx.topology.function.Supplier}
 */
@Deprecated
public interface Supplier<T> extends 
   com.ibm.streamsx.topology.function.Supplier<T> { }
