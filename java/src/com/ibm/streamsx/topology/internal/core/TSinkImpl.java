/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

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
    
    /* Placement control */
    private PlacementInfo placement;
    
    @Override
    public boolean isPlaceable() {
        return true;
    }
    
    private PlacementInfo getPlacementInfo() {
        if (placement == null)
            placement = PlacementInfo.getPlacementInfo(this);
        return placement;
    }

    @Override
    public TSink colocate(Placeable<?>... elements) {
        getPlacementInfo().colocate(this, elements);
        return this;
    }

    @Override
    public TSink addResourceTags(String... tags) {
        getPlacementInfo().addResourceTags(this, tags);
        return this;              
    }

    @Override
    public Set<String> getResourceTags() {
        return getPlacementInfo() .getResourceTags(this);
    }

}
