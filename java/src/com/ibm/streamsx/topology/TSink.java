/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import com.ibm.streamsx.topology.builder.BOperatorInvocation;

/**
 * A {@link TopologyElement} handle to a {@code sink} operation.
 * 
 * @see TStream#sink(com.ibm.streamsx.topology.function.Consumer)
 */
public interface TSink extends TopologyElement {
    
    /**
     * Get the operator associated with the sink.
     * @return the operator
     */
    BOperatorInvocation operator();
}
