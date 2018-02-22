/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018  
 */
package com.ibm.streamsx.topology.internal.tester.conditions;

import com.ibm.streamsx.topology.Topology;

/**
 * User conditions can be independent of a Stream. 
 * They implement this interface which allows arbitary
 * addition of items to the graph to fulfill the condition.
 */
public interface NoStreamCondition {
    
    /**
     * Add any elements to the topology to represent this condition. 
     * 
     * In a REST tester environment the condition name must be used
     * as the base name for the condition metrics.
     * 
     * @param topology Topology.
     * @param conditionName Name of the condition.
     */
    void addTo(Topology topology, String conditionName);
}
