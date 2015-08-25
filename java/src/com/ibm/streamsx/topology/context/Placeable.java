/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
*/
 package com.ibm.streamsx.topology.context;

import java.util.Set;

import com.ibm.streamsx.topology.TopologyElement;

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
     * Fuse this container with other topology elements so that
     * at runtime they all execute within the same operating system process.
     * At runtime the process will require a resource that has been tagged with
     * all the {@link #addResourceTags(String...) resource tags added} to each fused element.
     * 
     * @param elements Elements to fuse with this container.
     * 
     * @return this
     */
    public T fuse(Placeable<?> ... elements);
    
    /**
     * Add required resource tags for this topology element for distributed submission.
     * This topology element and any it has been {@link #fuse(Placeable...) fused}
     * with will execute on a resource (host) that has all the tags returned by
     * {@link #getResourceTags()}.
     * 
     * @param tags Tags to be required at runtime.
     * @return this
     */
    public T addResourceTags(String ... tags);
    
    /**
     * Get the set of resource tags this element requires.
     * If this topology element has been {@link #fuse(Placeable...) fused}
     * with other topology elements then the returned set is the union
     * of all {@link #addResourceTags(String...) resource tags added} to each fused element.
     * @return Read-only set of host tags this element requires.
     */
    public Set<String> getResourceTags();
}
