/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;
import com.ibm.streamsx.topology.tester.Condition;

/**
 * Separation of the runtime aspects of a
 * Tester from definition time.
 *
 */
public abstract class TesterRuntime {
    public enum TestState {
        NOT_READY,
        NO_PROGRESS,
        PROGRESS,
        VALID,
        FAIL,
    }



    private final ConditionTesterImpl tester;
    
    protected TesterRuntime(ConditionTesterImpl tester) {
        this.tester = tester;
    }
    
    protected ConditionTesterImpl tester() {
        return tester;
    }
    
    protected Topology topology() {
        return tester().getTopology();
    }
    
    public abstract void start(Object info) throws Exception;

    public abstract void shutdown(Future<?> future) throws Exception;
    
    public abstract void finalizeTester(Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers,
            Map<TStream<?>, Set<UserCondition<?>>> conditions) throws Exception;  
      
    /**
     * 
     * Check the state of the test. Method should only block for  limited time e.g. 1 second.
     * 
     * If endCondition becomes valid then the method should return with a state
     * reflecting the state of all conditions.
     * 
     * A method may return before endCondition is valid if it is known
     * the test will fail.
     * 
     * @param context
     *            Context job was submitted to.
     * @param config
     *            Configuration used for submit.
     * @param future
     *            Future for submitted job.
     * @param endCondition
     *            Condition the test should complete on.
     */
    public abstract TestState checkTestState(StreamsContext<?> context, Map<String, Object> config, Future<?> future,
            Condition<?> endCondition) throws Exception;
}