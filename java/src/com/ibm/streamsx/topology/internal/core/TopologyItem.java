/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

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
}
