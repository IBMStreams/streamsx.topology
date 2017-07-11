/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester;

import java.util.Map;
import java.util.Set;

import com.ibm.streams.flow.handlers.StreamHandler;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.tester.conditions.UserCondition;

/**
 * Separation of the runtime aspects of a
 * Tester from definition time.
 *
 */
public abstract class TesterRuntime {
    private final TupleCollection tester;
    
    protected TesterRuntime(TupleCollection tester) {
        this.tester = tester;
    }
    
    protected TupleCollection tester() {
        return tester;
    }
    
    protected Topology topology() {
        return tester().getTopology();
    }
    
    public abstract void start();

    public abstract void shutdown() throws Exception;
    
    public abstract void finalizeTester(Map<TStream<?>, Set<StreamHandler<Tuple>>> handlers,
            Map<TStream<?>, Set<UserCondition<?>>> condition) throws Exception;  
}