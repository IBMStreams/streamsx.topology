/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test;

import com.ibm.streamsx.topology.function.Predicate;

@SuppressWarnings("serial")
public class AllowAll<T> implements Predicate<T> {

    @Override
    public boolean test(T tuple) {
        return true;
    }

}
