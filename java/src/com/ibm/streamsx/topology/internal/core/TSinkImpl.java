/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import java.util.Objects;
import java.util.Set;

import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.context.Placeable;

/**
 * TSink implementation.
 */
public class TSinkImpl extends TopologyItem implements TSink {
    private final BOperatorInvocation op;

    public TSinkImpl(TopologyElement te, BOperatorInvocation op) {
        super(te);
        this.op = op;
    }

    @Override
    public BOperatorInvocation operator() {
        return op;
    }
        
    @Override
    public boolean isPlaceable() {
        return true;
    }

    @Override
    public TSink colocate(Placeable<?>... elements) {
        PlacementInfo.colocate(this, elements);
        return this;
    }

    @Override
    public TSink addResourceTags(String... tags) {
        PlacementInfo.addResourceTags(this, tags);
        return this;              
    }

    @Override
    public Set<String> getResourceTags() {
        return PlacementInfo.getResourceTags(this);
    }
    
    @Override
    public TSink invocationName(String name) {
        if (!isPlaceable())
            throw new IllegalStateException();
        
        builder().renameOp(operator(), Objects.requireNonNull(name));
        return this;
    }
}
