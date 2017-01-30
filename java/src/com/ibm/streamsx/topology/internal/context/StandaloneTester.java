/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.tester.StandaloneTesterContextFuture;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;

public class StandaloneTester extends StandaloneStreamsContext {

    @Override
    public Type getType() {
        return Type.STANDALONE_TESTER;
    }

    @Override
    public Future<Integer> _submit(Topology app, Map<String, Object> config)
            throws Exception {
        Future<Integer> standalone = super._submit(app, config);

        return new StandaloneTesterContextFuture<Integer>(standalone,
                (TupleCollection) (app.getTester()));
    }

    @Override
    void preInvoke(Topology app) {
        if (app.hasTester()) {
            TupleCollection collector = (TupleCollection) app.getTester();
            collector.startLocalCollector();
        }
    }
}
