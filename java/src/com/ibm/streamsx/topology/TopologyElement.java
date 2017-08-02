/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology;

import com.ibm.streamsx.topology.builder.GraphBuilder;

/**
 * Any element in a {@link Topology}.
 * 
 * All elements in a {@code Topology}, including {@code Topology} itself,
 * implement {@code TopologyElement}. <BR>
 * Some methods, typically those that create source streams, require a
 * {@code TopologyElement} to identify the topology that the new element needs
 * to be added to. Any other {@link TStream} or {@link TWindow} can be used as
 * the {@code TopologyElement} passed to those methods, rather than having to
 * pass the {@code Topology} reference all throughout the code that builds the
 * topology.
 * 
 */
public interface TopologyElement {
    /**
     * The topology for this element.
     * 
     * @return The topology for this element.
     */
    Topology topology();

    /**
     * Get the underlying {@code OperatorGraph}. Internal use only.
     * <BR>
     * Not intended to be called by applications, may be removed at any time.
     */
    GraphBuilder builder();
}
