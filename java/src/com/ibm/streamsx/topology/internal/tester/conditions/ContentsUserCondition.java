/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions;

import java.util.Collections;
import java.util.List;

public final class ContentsUserCondition<T> extends UserCondition<List<T>> {
    
    private final Class<T> clazz;
    private final List<T> expected;
    private final boolean ordered;
    
    public ContentsUserCondition(Class<T> clazz, List<T> expected, boolean ordered) {
        super(Collections.emptyList());
        this.clazz = clazz;
        this.expected = expected;
        this.ordered = ordered;
    }
    
    public Class<T> getTupleClass() {
        return clazz;
    }
    public boolean isOrdered() {
        return ordered;
    }
    public List<T> getExpected() {
        return expected;
    }
    @Override
    public String toString() {
        String result;
        if (getImpl() == null)
            result = getResult().toString();
        else
            result = getImpl().toString();
        
        return "Received Tuples: " + result;
    }
    
    
}
