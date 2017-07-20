/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions;

import com.ibm.streamsx.topology.tester.Condition;

/**
 * Condition that gets returned to the caller.
 * 
 * Under the covers it delegates to a implementation 
 * condition that is created once the job under test is submitted.
 */
public abstract class UserCondition<R> implements Condition<R> {
    
    private Condition<R> impl;
    private final R noResult;
    
    UserCondition(R noResult) {
        this.noResult = noResult;
    }
    
    public synchronized final void setImpl(Condition<R> impl) {
        this.impl = impl;
    }
    
    synchronized Condition<R> getImpl() {
        return impl;
    }

    @Override
    public final boolean valid() {
        if (getImpl()  == null)
            return false;
        return getImpl().valid();
    }
    
    @Override
    public final boolean failed() {
        if (getImpl()  == null)
            return false;
        return getImpl().failed();
    }

    @Override
    public final R getResult() {
        if (getImpl() == null)
            return noResult;
        return getImpl().getResult();
    }
    
    @Override
    public String toString() {
        if (getImpl() != null)
            return getImpl().toString();
        return super.toString();
    }
}
