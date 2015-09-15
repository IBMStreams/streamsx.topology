/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.functional.ops.SubmissionParameterManager;
import com.ibm.streamsx.topology.internal.tester.TupleCollection;

public class EmbeddedTester extends StreamsContextImpl<JavaTestableGraph> {

    private final JavaOperatorTester jot = new JavaOperatorTester();

    @Override
    public Type getType() {
        return Type.EMBEDDED_TESTER;
    }

    @Override
    public Future<JavaTestableGraph> submit(Topology app,
            Map<String, Object> config) throws Exception {

        app.builder().checkSupportsEmbeddedMode();
        
        SubmissionParameterManager.initializeEmbedded(app.builder(), config);
        
        JavaTestableGraph tg = jot.executable(app.graph());

        TupleCollection tester = (TupleCollection) app.getTester();
        tester.setupEmbeddedTestHandlers(tg);

        return tg.execute();
    }

    @Override
    public boolean isSupported(Topology topology) {
        try {
            topology.builder().checkSupportsEmbeddedMode();
            return true;
        } catch(IllegalStateException e) {
            return false;
        }
    }

}
