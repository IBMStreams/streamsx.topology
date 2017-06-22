/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017 
 */
package com.ibm.streamsx.topology.internal.tester;

/**
 * Separation of the runtime aspects of a
 * Tester from definition time.
 *
 */
public class TesterRuntime {
    private final TupleCollection tester;
    
    protected TesterRuntime(TupleCollection tester) {
        this.tester = tester;
    }
    
    public void start() {
        tester.startLocalCollector();
    }

    public void shutdown() throws Exception {
        tester.shutdown();  
    }
}