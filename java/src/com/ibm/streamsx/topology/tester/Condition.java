/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.tester;

/**
 * Function representing if a condition is valid or not.
 * 
 */
public interface Condition<T> {
    boolean valid();
    T getResult();
}
