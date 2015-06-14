/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.tester.StandaloneTesterContextFuture;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;

public class StandaloneTester extends StandaloneStreamsContext implements AutoCloseable {

    /**
     * tg and testerFuture are for the local testing graph
     * that is collecting the tuples from the topology under test.
     */
    private JavaTestableGraph tg;
    Future<JavaTestableGraph> testerFuture;

    @Override
    public Type getType() {
        return Type.STANDALONE_TESTER;
    }

    @Override
    public Future<Integer> submit(Topology app, Map<String, Object> config)
            throws Exception {
        Future<Integer> standalone = super.submit(app, config);

        return new StandaloneTesterContextFuture<Integer>(standalone, this);
    }

    @Override
    public void close() throws Exception {

        if (!testerFuture.isDone())
            testerFuture.cancel(true);

        TupleCollection tc = (TupleCollection) bundler.graphItems
                .get("testerCollector");
        tc.shutdown();

    }

    @Override
    void preInvoke() {

        tg = (JavaTestableGraph) bundler.graphItems.get("testerGraph");
        testerFuture = tg.execute();
        super.preInvoke();
    }
}
