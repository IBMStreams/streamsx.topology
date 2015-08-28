/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import com.ibm.streamsx.topology.builder.BOperatorInvocation;
import com.ibm.streamsx.topology.context.Placeable;

/**
 * A handle to the {@code sink} of a {@code TStream}.
 * 
 * @see TStream#sink(com.ibm.streamsx.topology.function.Consumer)
 */
public interface TSink extends TopologyElement, Placeable<TSink> {
    
    /**
     * Get the operator associated with the sink.
     * @return the operator
     */
    BOperatorInvocation operator();
}
