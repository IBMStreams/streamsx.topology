package com.ibm.streamsx.topology.internal.tester.rest;

import com.ibm.streamsx.topology.Topology;

public interface NoStreamCondition {
    
    public void addTo(Topology topology, String conditionName);
}
