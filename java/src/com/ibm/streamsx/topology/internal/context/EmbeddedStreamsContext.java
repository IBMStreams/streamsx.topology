/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.context;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streams.flow.javaprimitives.JavaOperatorTester;
import com.ibm.streams.flow.javaprimitives.JavaTestableGraph;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.internal.embedded.EmbeddedGraph;
import com.ibm.streamsx.topology.internal.functional.ops.SubmissionParameterManager;

public class EmbeddedStreamsContext extends
        StreamsContextImpl<JavaTestableGraph> {

    

    @Override
    public Type getType() {
        return Type.EMBEDDED;
    }

    @Override
    public Future<JavaTestableGraph> submit(Topology app,
            Map<String, Object> config) throws Exception {
        
        app.finalizeGraph(getType());
        
        config = new HashMap<>(config);

        EmbeddedGraph eg = new EmbeddedGraph(app.builder());
        eg.verifySupported();
        
        SubmissionParameterManager.initializeEmbedded(app.builder(), config);
        
        // Declare the mock framework graph.
        OperatorGraph dg = eg.declareGraph();
        
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
