/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.rest;

import java.io.IOException;

import com.ibm.streamsx.rest.Metric;
import com.ibm.streamsx.topology.internal.tester.TesterRuntime;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.tester.Condition;

public class MetricCondition<T> implements Condition<T> {

    final String name;
       
    private Metric valid;
    private Metric seq;
    private Metric fail;
    long lastSeq;
    private TesterRuntime.TestState state;
    private boolean frozen;

    public MetricCondition(String name, UserCondition<T> userCondition) {
        this.name = name;
        setState(TesterRuntime.TestState.NOT_READY);
        userCondition.setImpl(this);
    }
    
    /**
     * Maintains the state of the condition
     * after the job is canceled.
     */
    synchronized void freeze() throws IOException {
        frozen = true;
        if (seq != null) {
            seq.refresh();
            lastSeq = seq.getValue();
        }
        valid = seq = fail = null;      
    }
    synchronized boolean isFrozen() {
        return frozen;
    }
    

    @Override
    public synchronized boolean valid() {
        return getState() == TesterRuntime.TestState.VALID;
    }
    
    @Override
    public boolean failed() {
        return getState() == TesterRuntime.TestState.FAIL;
    }
    
    @Override
    public T getResult() {
        throw new UnsupportedOperationException();
    }

    synchronized boolean hasMetrics() {
        return valid != null && seq != null && fail != null;
    }
    
    synchronized void setValidMetric(Metric valid) {
        this.valid = valid;
    }
    synchronized void setSeqMetric(Metric seq) {
        this.seq = seq;
    }
    synchronized void setFailMetric(Metric fail) {      
        this.fail = fail;
    }
    
    private synchronized TesterRuntime.TestState setState(TesterRuntime.TestState state) {
        return this.state = state;
    }
    
    synchronized TesterRuntime.TestState getState() {
        return this.state;
    }

    /**
     * Check the condition based upon the metrics.
     * 
     * Outcome is one of the states:
     */
    TesterRuntime.TestState oneCheck() throws IOException {
        if (isFrozen())
            return getState();
        
        if (!hasMetrics())
            return setState(TesterRuntime.TestState.NOT_READY);
        
        if (getState() == TesterRuntime.TestState.FAIL)
            return TesterRuntime.TestState.FAIL;

        fail.refresh();
        valid.refresh(); 
        seq.refresh();
        
        if (fail.getValue() != 0L) {
            return setState(TesterRuntime.TestState.FAIL);
        }     
        
        if (valid.getValue() == 1L)
            return setState(TesterRuntime.TestState.VALID);
        
        long s = seq.getValue();
        synchronized (this) {          
            if (s == lastSeq)
                return setState(TesterRuntime.TestState.NO_PROGRESS);
            lastSeq = s;
            return setState(TesterRuntime.TestState.PROGRESS);
        }
    }
    
    @Override
    public String toString() {
        return "Result not available";
    }
}
