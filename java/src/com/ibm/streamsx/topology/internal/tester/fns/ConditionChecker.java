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
import com.ibm.streamsx.topology.internal.tester.rest.RESTTesterRuntime;
import com.ibm.streamsx.topology.tester.Tester;

public abstract class ConditionChecker<T> implements Consumer<T>, Initializable {
    
    static Logger TEST_TRACE = Logger.getLogger(Tester.class.getName());
    
    public static final String METRIC_PREFIX = "streamsx.condition:";
    
    public static String metricName(String type, String name) {
        return METRIC_PREFIX + type + ":" + name;
    }

    private static final long serialVersionUID = 1L;
    
    private final String name;
    private transient AtomicBoolean valid;
    private transient AtomicLong seq;
    private transient AtomicBoolean fail;
    private transient long count;
    
    public ConditionChecker(String name) {
        this.name = name;
    }
    
    @Override
    public void initialize(FunctionContext functionContext) throws Exception {
        
        valid = new AtomicBoolean();
        seq = new AtomicLong();
        fail = new AtomicBoolean();
       
        functionContext.createCustomMetric(metricName("valid", name),
                "Condition: " + name + " is valid", "gauge",
                () -> valid.get() ? 1L: 0L);
        
        functionContext.createCustomMetric(metricName("seq", name),
                "Condition: " + name + " sequence", "counter",
                seq::get);
        
        functionContext.createCustomMetric(metricName("fail", name),
                "Condition: " + name + " failed", "gauge",
                () -> fail.get() ? 1L: 0L);       
    }
    
    /**
     * Once failed the condition can never become valid.
     * @throws InterruptedException 
     */
    void setFailed(String why) {
        TEST_TRACE.severe(name + ": " + why);
        fail.set(true);
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
        if (!fail.get()) {
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
        return fail.get();
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
