/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import com.ibm.streams.flow.declare.OperatorGraph;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.GraphBuilder;

public abstract class TopologyItem implements TopologyElement {
    private final TopologyElement te;

    protected TopologyItem(TopologyElement te) {
        this.te = te;
    }

    @Override
    public final Topology topology() {
        return te.topology();
    }

    @Override
    public final GraphBuilder builder() {
        return te.builder();
    }

    @Override
    public OperatorGraph graph() {
        return te.graph();
    }
}
