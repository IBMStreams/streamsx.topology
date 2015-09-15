/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
*/
 package com.ibm.streamsx.topology.context;

import java.util.Set;

import com.ibm.streamsx.topology.TopologyElement;
import com.ibm.streamsx.topology.builder.BOperatorInvocation;

/**
 * Placement directives for a topology element when executing
 * in a distributed runtime.
 * <BR>
 * Placement directives only apply when the topology
 * is submitted to a {@link StreamsContext.Type#DISTRIBUTED DISTRIBUTED},
 * {@link StreamsContext.Type#ANALYTICS_SERVICE ANALYTICS_SERVICE}
 * or {@link StreamsContext.Type#DISTRIBUTED_TESTER DISTRIBUTED_TESTER} context.
 * <BR>
 * For all other context types directives are ignored.
 */
public interface Placeable<T extends Placeable<T>> extends TopologyElement {
    
    /**
     * Can this element have placement directives applied to it.
     * @return {@code true} if placement directives can be assigned, {@code false} if it can not.
     */
    boolean isPlaceable();
    
    /**
     * Colocate this element with other topology elements so that
     * at runtime they all execute within the same operating system process.
     * {@code elements} may contain any {@code Placeable} within
     * the same topology, there is no requirement
     * that the element is connected (by a stream) directly or indirectly
     * to this element.
     * 
     * @param elements Elements to colocate with this container.
     * 
     * @return this
     * 
     * @throws IllegalStateExeception Any element including {@code this} returns {@code false} for {@link #isPlaceable()}.
     */
    T colocate(Placeable<?> ... elements);
    
    /**
     * Add required resource tags for this topology element for distributed submission.
     * This topology element and any it has been {@link #colocate(Placeable...) colocated}
     * with will execute on a resource (host) that has all the tags returned by
     * {@link #getResourceTags()}.
     * 
     * @param tags Tags to be required at runtime.
     * @return this
     */
    T addResourceTags(String ... tags);
    
    /**
     * Get the set of resource tags this element requires.
     * If this topology element has been {@link #colocate(Placeable...) colocated}
     * with other topology elements then the returned set is the union
     * of all {@link #addResourceTags(String...) resource tags added} to each colocated element.
     * @return Read-only set of host tags this element requires.
     */
    Set<String> getResourceTags();
    
    BOperatorInvocation operator(); 
}
