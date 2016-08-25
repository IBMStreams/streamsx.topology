/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.test;

import com.ibm.streamsx.topology.function.UnaryOperator;

/**
 * A functional operator to delay a stream's first tuple;
 * subsequent tuples are not delayed.
 *
 * @param <T>
 */
public class InitialDelay<T> implements UnaryOperator<T> {
    private static final long serialVersionUID = 1L;
    private long initialDelayMsec;
    
    /**
     * @param delayMsec
     */
    public InitialDelay(long delayMsec) {
        if (delayMsec < 0)
            throw new IllegalArgumentException("delayMsec");
        this.initialDelayMsec = delayMsec;
    }
    
    @Override
    public T apply(T v) {
        if (initialDelayMsec != -1) {
            try {
                Thread.sleep(initialDelayMsec);
            } catch (InterruptedException e) {
                // Force parent thread to terminate
                Thread.currentThread().interrupt();
                return null;
            }
            initialDelayMsec = -1;
        }
        return v;
    }

}
