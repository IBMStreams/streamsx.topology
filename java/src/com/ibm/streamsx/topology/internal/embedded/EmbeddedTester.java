/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.embedded;

import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.context.StreamsContextImpl;
import com.ibm.streamsx.topology.internal.tester.ConditionTesterImpl;

public class EmbeddedTester extends StreamsContextImpl<JavaTestableGraph> {

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
        
        EmbeddedGraph.initializeEmbedded(app.builder(), config);
                
        if (tester != null)
            tester.getRuntime().start(eg);
        return eg.execute();
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
