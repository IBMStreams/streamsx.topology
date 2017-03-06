/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
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
    void preInvoke(AppEntity entity, File bundle) {
        Topology app = entity.app;
        if (app != null && app.hasTester()) {
            TupleCollection collector = (TupleCollection) app.getTester();
            collector.startLocalCollector();
        }
    }

    @Override
    Future<Integer> postSubmit(AppEntity entity, Future<Integer> future) {
        Topology app = entity.app;
        if (app == null)
            return future;
        return new StandaloneTesterContextFuture<Integer>(future,
                (TupleCollection) (app.getTester()));
    }
}
