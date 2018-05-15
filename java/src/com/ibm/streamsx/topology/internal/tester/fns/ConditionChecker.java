/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester.fns;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.ibm.streamsx.topology.function.Consumer;
import com.ibm.streamsx.topology.function.FunctionContext;
import com.ibm.streamsx.topology.function.Initializable;
import com.ibm.streamsx.topology.tester.Tester;

public abstract class ConditionChecker<T> implements Consumer<T>, Initializable {
    
    static Logger TEST_TRACE = Logger.getLogger(Tester.class.getName());
    
    public static final String METRIC_PREFIX = "streamsx.condition:";
    
    public static String metricName(String type, String name) {
        return METRIC_PREFIX + type + ":" + name;
    }

    private static final long serialVersionUID = 1L;
    
    private final String name;
    private final AtomicBoolean valid = new AtomicBoolean();
    private final AtomicLong seq = new AtomicLong();
    private final AtomicBoolean fail = new AtomicBoolean();
    private long count;
    
    /**
     * We need an addition transient fail status so that in the case of a
     * reset after a consistent region checkpoint we don't reset the state
     * to be non-failed.
     * 
     * Assumption is currently that a PE failure/restart always causes
     * the test to fail, so the window of a condition fails but
     * the PE fails before the tester notices is not currently a hidden failure.
     * 
     * fail is persistent so that if the PE does restart we maintain
     * at best effort that we've already seen a failure.
     */
    private transient AtomicBoolean failSinceStart;
    
    public ConditionChecker(String name) {
        this.name = name;
    }
    
    @Override
    public void initialize(FunctionContext functionContext) throws Exception {
        
        failSinceStart = new AtomicBoolean();
       
        functionContext.createCustomMetric(metricName("valid", name),
                "Condition: " + name + " is valid", "gauge",
                () -> valid.get() ? 1L: 0L);
        
        functionContext.createCustomMetric(metricName("seq", name),
                "Condition: " + name + " sequence", "counter",
                seq::get);
        
        functionContext.createCustomMetric(metricName("fail", name),
                "Condition: " + name + " failed", "gauge",
                () -> failed() ? 1L: 0L);       
    }
    
    /**
     * Once failed the condition can never become valid.
     * @throws InterruptedException 
     */
    void setFailed(String why) {
        TEST_TRACE.severe(name + ": " + why);
        fail.set(true);
        failSinceStart.set(true);
        valid.set(false);
    }
    
    void failTooMany(long expected) {
        String why = String.format("Too many tuples: Expected %d, received %d.", expected, this.tupleCount());
        setFailed(why);
    }
    
    void failUnexpectedTuple(T tuple, List<T> expected) {
        setFailed(String.format("Tuple %s not expected, not in: %s", tuple, expected));
    }   
    
    void setValid() {
        if (!failed()) {
            valid.set(true);
        }
    }
    
    void progress() {
        seq.incrementAndGet();
    }
    
    long tupleCount() {
        return count;
    }
    
    boolean failed() {
        return fail.get() || failSinceStart.get();
    }
    
    @Override
    public final void accept(T tuple) {
        if (failed())
            return;
        
        progress();
        count++;
        checkValid(tuple);
    }
    
    abstract void checkValid(T tuple);
}
