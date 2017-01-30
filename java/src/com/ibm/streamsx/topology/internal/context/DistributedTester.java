/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.math.BigInteger;
import java.util.Map;
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
    Future<BigInteger> _submit(Topology app, Map<String, Object> config)
            throws Exception {
        Future<BigInteger> distributed = super._submit(app, config);

        return new DistributedTesterContextFuture(distributed.get(),
                (TupleCollection) app.getTester());
    }

    @Override
    void preInvoke(Topology app) {
        
        if (app.hasTester()) {
            TupleCollection collector = (TupleCollection) app.getTester();
            collector.startLocalCollector();
        }
    }
}
