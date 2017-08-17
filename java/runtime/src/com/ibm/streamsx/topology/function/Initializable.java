/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.function;

/**
 * Optional interface that a function can implement
 * to perform initialization.
 */
public interface Initializable {
    
    void initialize(FunctionContext functionContext) throws Exception;

}
