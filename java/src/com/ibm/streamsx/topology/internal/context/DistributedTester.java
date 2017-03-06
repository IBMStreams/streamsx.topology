/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.io.File;
import java.math.BigInteger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.tester.DistributedTesterContextFuture;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;

public class DistributedTester extends DistributedStreamsContext {

    @Override
    public Type getType() {
        return Type.DISTRIBUTED_TESTER;
    }
    
    @Override
    Future<BigInteger> postSubmit(AppEntity entity, Future<BigInteger> future) throws InterruptedException, ExecutionException {
        Topology app = entity.app;
        if (app == null)
            return future;
        return new DistributedTesterContextFuture(future.get(),
                (TupleCollection) (app.getTester()));
    }
    
    

    @Override
    void preInvoke(AppEntity entity, File bundle) {
        Topology app = entity.app;
        if (app != null && app.hasTester()) {
            TupleCollection collector = (TupleCollection) app.getTester();
            collector.startLocalCollector();
        }
    }
}
