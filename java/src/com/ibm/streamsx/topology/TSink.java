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
 * <P>
 * {@code TSink} implements {@link Placeable} to allow placement
 * directives against its {@link TStream#forEach(com.ibm.streamsx.topology.function.Consumer) sink()}
 * processing.
 * Calling a {@code Placeable} method on {@code this}
 * will apply to the container that is executing the {@code Consumer}
 * passed into {@code sink()}.
 * </P>
 * 
 * @see TStream#forEach(com.ibm.streamsx.topology.function.Consumer)
 */
public interface TSink extends TopologyElement, Placeable<TSink> {
    
    /**
     * Internal method.
     * <BR>
     * <B><I>Not intended to be called by applications, may be removed at any time.</I></B>
     * <BR>
     * Get the operator associated with the sink.
     * @return the operator
     */
    BOperatorInvocation operator();
}
