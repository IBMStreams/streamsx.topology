/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017  
 */
package com.ibm.streamsx.topology.internal.tester.conditions.handlers;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Condition implementation that uses a StreamHandler.
 *
 * @param <R> Return type of condition.
 * @param <H> StreamHandler type
 * @param <U> UserCondition type
 */
public abstract class HandlerCondition<R, H extends StreamHandler<Tuple>, U extends UserCondition<R>> implements Condition<R> {
    
    final U userCondition;
    final H handler;
    private boolean failed;
    
    HandlerCondition(U userCondition, H handler) {
        this.handler = handler;
        this.userCondition = userCondition;
        userCondition.setImpl(this);
    }
    
    @Override
    public synchronized boolean failed() {
        return failed;
    }
    synchronized void fail() {
        failed = true;
    }
    
    @Override
    public final String toString() {
        return getResult().toString();
    }
}
