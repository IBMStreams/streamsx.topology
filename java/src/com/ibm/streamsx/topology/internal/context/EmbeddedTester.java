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
import com.ibm.streamsx.topology.internal.embedded.EmbeddedGraph;
import com.ibm.streamsx.topology.internal.functional.ops.SubmissionParameterManager;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;

public class EmbeddedTester extends StreamsContextImpl<JavaTestableGraph> {

    private final JavaOperatorTester jot = new JavaOperatorTester();

    @Override
    public Type getType() {
        return Type.EMBEDDED_TESTER;
    }

    @Override
    public Future<JavaTestableGraph> submit(Topology app,
            Map<String, Object> config) throws Exception {

        EmbeddedGraph eg = new EmbeddedGraph(app.builder());
        eg.verifySupported();
        
        ConditionTesterImpl tester = null;
        if (app.hasTester()) {
            tester = (ConditionTesterImpl) app.getTester();
            if (tester.hasTests())
                tester.finalizeGraph(getType());
            else
                tester = null;
        }
        
        SubmissionParameterManager.initializeEmbedded(app.builder(), config);
        
        // TODO - actually use EmbeddedGRaph
        eg.declareGraph();
        
        JavaTestableGraph tg = jot.executable(app.graph());

        if (tester != null)
            tester.getRuntime().start(tg);
        return tg.execute();
    }

    @Override
    public boolean isSupported(Topology topology) {
        try {
            EmbeddedGraph.verifySupported(topology.builder());
            return true;
        } catch(IllegalStateException e) {
            return false;
        }
    }

}
