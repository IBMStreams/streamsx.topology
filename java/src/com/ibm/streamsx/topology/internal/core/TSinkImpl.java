/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.internal.core;

import com.ibm.streamsx.topology.TSink;
import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;

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

}
