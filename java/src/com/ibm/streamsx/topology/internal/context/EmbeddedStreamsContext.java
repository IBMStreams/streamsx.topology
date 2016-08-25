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

public class EmbeddedStreamsContext extends
        StreamsContextImpl<JavaTestableGraph> {

    private final JavaOperatorTester jot = new JavaOperatorTester();

    @Override
    public Type getType() {
        return Type.EMBEDDED;
    }

    @Override
    public Future<JavaTestableGraph> submit(Topology app,
            Map<String, Object> config) throws Exception {

        app.builder().checkSupportsEmbeddedMode();
        
        SubmissionParameterManager.initializeEmbedded(app.builder(), config);
        
        return jot.executable(app.graph()).execute();
        // jot.executable(app.graph()).executeToCompletion();
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
