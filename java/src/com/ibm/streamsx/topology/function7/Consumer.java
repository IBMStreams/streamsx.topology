/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.function7;

import java.io.Serializable;

/**
 * A function that takes an argument and returns nothing.
 * @deprecated Replaced by
 * {@link com.ibm.streamsx.topology.function.Consumer}
 */
@Deprecated
public interface Consumer<T> extends 
   com.ibm.streamsx.topology.function.Consumer<T> { }
