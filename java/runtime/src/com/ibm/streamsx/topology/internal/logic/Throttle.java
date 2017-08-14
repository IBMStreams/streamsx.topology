/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.logic;

import com.ibm.streamsx.topology.function.UnaryOperator;


public final class Throttle<T> implements UnaryOperator<T> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final long delayms;
    transient long nextTupleTime;

    public Throttle(long delayms) {
        this.delayms = delayms;
    }

    @Override
    public T apply(T v1) {
        long now = System.currentTimeMillis();
        if (nextTupleTime != 0) {
            if (now < nextTupleTime) {
                try {
                    Thread.sleep(nextTupleTime - now);
                } catch (InterruptedException e) {
                    // Force parent thread to terminate
                    Thread.currentThread().interrupt();
                    return null;
                }
                now = System.currentTimeMillis();
            }
        }
        nextTupleTime = now + delayms;
        return v1;
    }
}
